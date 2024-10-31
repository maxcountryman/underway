use std::env;

use async_openai::{
    config::OpenAIConfig,
    types::{ChatCompletionRequestMessage, CreateChatCompletionRequestArgs},
    Client,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sqlx::PgPool;
use tokio::task::JoinSet;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use underway::{job::Context, Job, To, ToTaskResult};

const SYSTEM_PROMPT: &str = include_str!("../system-prompt.llm.txt");

const HN_API_BASE: &str = "https://hacker-news.firebaseio.com/v0/";

#[derive(Deserialize, Serialize)]
struct Summarize {
    top_stories: Vec<(Story, Vec<Comment>)>,
}

#[derive(Clone)]
struct State {
    openai_client: Client<OpenAIConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Story {
    title: String,
    score: u32,
    kids: Option<Vec<u32>>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Comment {
    id: u32,
    text: Option<String>,
}

fn get_item_uri(item_id: u32) -> String {
    format!("{HN_API_BASE}item/{}.json", item_id)
}

async fn fetch_item_by_id<T>(id: u32) -> Result<T, reqwest::Error>
where
    T: DeserializeOwned,
{
    let story_url = get_item_uri(id);
    let item: T = reqwest::get(&story_url).await?.json().await?;
    Ok(item)
}

async fn fetch_comments(comment_ids: &[u32], max_comments: usize) -> Vec<Comment> {
    let mut tasks = JoinSet::new();
    for &id in comment_ids.iter().take(max_comments) {
        tasks.spawn(async move { fetch_item_by_id::<Comment>(id).await });
    }

    let mut comments = Vec::new();
    while let Some(task) = tasks.join_next().await {
        if let Ok(Ok(comment)) = task {
            comments.push(comment);
        }
    }

    comments
}

async fn fetch_top_stories() -> Result<Vec<(Story, Vec<Comment>)>, reqwest::Error> {
    let top_stories_url = format!("{HN_API_BASE}/topstories.json");
    let story_ids: Vec<u32> = reqwest::get(top_stories_url).await?.json().await?;

    let story_ids = &story_ids[..20];

    let mut tasks = JoinSet::new();
    for &id in story_ids {
        tasks.spawn(async move { fetch_item_by_id::<Story>(id).await });
    }

    let mut stories = Vec::new();
    while let Some(task) = tasks.join_next().await {
        if let Ok(Ok(story)) = task {
            stories.push(story);
        }
    }

    stories.sort_by(|a, b| b.score.cmp(&a.score));
    let top_stories = stories.into_iter().collect::<Vec<_>>();

    let mut top_stories_with_comments = Vec::new();
    for story in top_stories {
        let comment_ids = story.kids.as_deref().unwrap_or_default();
        let comments = fetch_comments(comment_ids, 5).await; // Fetch up to 5 comments per story
        top_stories_with_comments.push((story, comments));
    }

    Ok(top_stories_with_comments)
}

fn user_prompt(top_stories: &[(Story, Vec<Comment>)]) -> String {
    let mut prompt = String::new();
    for (story, comments) in top_stories {
        let comments_text = comments
            .iter()
            .filter_map(|c| c.text.as_deref())
            .collect::<Vec<_>>()
            .join("\n");

        let formatted_story = format!("title: {}\ncomments:\n{}\n\n", story.title, comments_text);
        prompt.push_str(&formatted_story);
    }

    prompt
}

async fn summarize(
    client: &Client<OpenAIConfig>,
    top_stories: &[(Story, Vec<Comment>)],
) -> Result<String, Box<dyn std::error::Error>> {
    let system_message = ChatCompletionRequestMessage::Assistant(SYSTEM_PROMPT.into());

    let input_prompt = user_prompt(top_stories);
    let user_message = ChatCompletionRequestMessage::User(input_prompt.into());

    let request = CreateChatCompletionRequestArgs::default()
        .model("gpt-3.5-turbo")
        .messages(vec![system_message, user_message])
        .max_tokens(800_u32)
        .build()?;

    let response = client.chat().create(request).await?;
    let summary = response
        .choices
        .first()
        .and_then(|choice| choice.message.content.clone())
        .unwrap_or_else(|| "No summary available.".to_string());

    Ok(summary)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize the tracing subscriber.
    tracing_subscriber::registry()
        .with(EnvFilter::new(
            env::var("RUST_LOG").unwrap_or_else(|_| "info,underway=info,sqlx=warn".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .try_init()?;

    let database_url = &env::var("DATABASE_URL").expect("DATABASE_URL should be set");
    let pool = PgPool::connect(database_url).await?;

    underway::run_migrations(&pool).await?;

    let openai_client = Client::new();

    let job = Job::builder()
        .state(State { openai_client })
        .step(|_cx, _| async move {
            tracing::info!("Retrieving the top five stories from Hacker News");

            let top_stories = fetch_top_stories().await.retryable()?;
            let top_five = top_stories.into_iter().take(5).collect::<Vec<_>>();

            To::next(Summarize {
                top_stories: top_five,
            })
        })
        .step(
            |Context {
                 state: State { openai_client },
                 ..
             },
             Summarize { top_stories }| async move {
                tracing::info!("Summarizing top five story discussions");

                let summary = summarize(&openai_client, &top_stories).await.retryable()?;
                println!("{}", summary);

                To::done()
            },
        )
        .name("example-rag")
        .pool(pool)
        .build()
        .await?;

    job.enqueue(&()).await?;

    job.start().await??;

    Ok(())
}
