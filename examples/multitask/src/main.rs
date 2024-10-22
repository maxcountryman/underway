use std::env;

use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Postgres, Transaction};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use underway::{
    queue::Error as QueueError,
    task::{Id as TaskId, Result as TaskResult},
    Queue, Task, Worker,
};

const QUEUE_NAME: &str = "example-multitask";

#[derive(Debug, Clone, Deserialize, Serialize)]
struct WelcomeEmail {
    user_id: i32,
    email: String,
    name: String,
}

struct WelcomeEmailTask;

impl WelcomeEmailTask {
    async fn enqueue(
        &self,
        pool: &PgPool,
        queue: &Queue<Multitask>,
        input: WelcomeEmail,
    ) -> Result<TaskId, QueueError> {
        // This ensures our task-specific configuration is applied.
        let welcome_email_task = self.into();
        queue
            .enqueue(pool, &welcome_email_task, TaskInput::WelcomeEmail(input))
            .await
    }
}

impl Task for WelcomeEmailTask {
    type Input = WelcomeEmail;
    type Output = ();

    async fn execute(&self, _tx: Transaction<'_, Postgres>, input: Self::Input) -> TaskResult<()> {
        tracing::info!(?input, "Simulate sending a welcome email");
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Order {
    user_id: i32,
    sku: String,
}

struct OrderTask;

impl OrderTask {
    async fn enqueue(
        &self,
        pool: &PgPool,
        queue: &Queue<Multitask>,
        input: Order,
    ) -> Result<TaskId, QueueError> {
        // This ensures our task-specific configuration is applied.
        let order_task = self.into();
        queue
            .enqueue(pool, &order_task, TaskInput::Order(input))
            .await
    }
}

impl Task for OrderTask {
    type Input = Order;
    type Output = ();

    async fn execute(&self, _tx: Transaction<'_, Postgres>, input: Self::Input) -> TaskResult<()> {
        tracing::info!(?input, "Simulate order processing");
        Ok(())
    }

    fn priority(&self) -> i32 {
        10 // We'll make Order tasks higher priority.
    }
}

#[derive(Clone, Deserialize, Serialize)]
enum TaskInput {
    WelcomeEmail(WelcomeEmail),
    Order(Order),
}

struct Multitask {
    welcome_email: WelcomeEmailTask,
    order: OrderTask,
    priority: i32,
}

impl Multitask {
    fn new() -> Self {
        Self {
            welcome_email: WelcomeEmailTask,
            order: OrderTask,
            priority: 0, // This is set when we convert from one of our tasks.
        }
    }
}

impl From<&WelcomeEmailTask> for Multitask {
    fn from(welcome_email_task: &WelcomeEmailTask) -> Self {
        Self {
            welcome_email: WelcomeEmailTask,
            order: OrderTask,
            priority: welcome_email_task.priority(), // Proxy task-specific configuration.
        }
    }
}

impl From<&OrderTask> for Multitask {
    fn from(order_task: &OrderTask) -> Self {
        Self {
            welcome_email: WelcomeEmailTask,
            order: OrderTask,
            priority: order_task.priority(), // Proxy task-specific configuration.
        }
    }
}

impl Task for Multitask {
    type Input = TaskInput;
    type Output = ();

    async fn execute(&self, tx: Transaction<'_, Postgres>, input: Self::Input) -> TaskResult<()> {
        match input {
            TaskInput::WelcomeEmail(input) => self.welcome_email.execute(tx, input).await,
            TaskInput::Order(input) => self.order.execute(tx, input).await,
        }
    }

    fn priority(&self) -> i32 {
        self.priority
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize the tracing subscriber.
    tracing_subscriber::registry()
        .with(EnvFilter::new(
            env::var("RUST_LOG").unwrap_or_else(|_| "debug,underway=info,sqlx=warn".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .try_init()?;

    // Set up the database connection pool.
    let database_url = &env::var("DATABASE_URL").expect("DATABASE_URL should be set");
    let pool = PgPool::connect(database_url).await?;

    // Run migrations.
    underway::MIGRATOR.run(&pool).await?;

    // Create the task queue.
    let queue = Queue::builder()
        .name(QUEUE_NAME)
        .pool(pool.clone())
        .build()
        .await?;

    // Enqueue a welcome email task.
    let welcome_email_task = WelcomeEmailTask;
    let task_id = welcome_email_task
        .enqueue(
            &pool,
            &queue,
            WelcomeEmail {
                user_id: 42,
                email: "ferris@example.com".to_string(),
                name: "Ferris".to_string(),
            },
        )
        .await?;

    tracing::info!(task.id = %task_id.as_hyphenated(), "Enqueued welcome email task");

    // Enqueue an order task.
    let order_task = OrderTask;
    let task_id = order_task
        .enqueue(
            &pool,
            &queue,
            Order {
                user_id: 42,
                sku: "SKU0-0042".to_string(),
            },
        )
        .await?;

    tracing::info!(task.id = %task_id.as_hyphenated(), "Enqueued order task");

    // Run a worker that processes all tasks.
    let multitask = Multitask::new();
    Worker::new(queue, multitask).run().await?;

    Ok(())
}
