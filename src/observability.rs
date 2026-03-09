use opentelemetry::{
    trace::{
        SpanContext as OtelSpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState,
    },
    Context,
};
use serde::{
    de::Error as _, ser::SerializeStruct, Deserialize, Deserializer, Serialize, Serializer,
};
use tracing_opentelemetry::OpenTelemetrySpanExt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SpanContext {
    trace_id: TraceId,
    span_id: SpanId,
    sampled: bool,
}

impl SpanContext {
    pub(crate) fn capture_current() -> Option<Self> {
        let context = tracing::Span::current().context();
        let span = context.span();
        let span_context = span.span_context();
        if !span_context.is_valid() {
            return None;
        }

        Some(Self {
            trace_id: span_context.trace_id(),
            span_id: span_context.span_id(),
            sampled: span_context.is_sampled(),
        })
    }

    pub(crate) fn set_parent(&self, span: &tracing::Span) -> String {
        let trace_flags = if self.sampled {
            TraceFlags::SAMPLED
        } else {
            TraceFlags::NOT_SAMPLED
        };
        let span_context = OtelSpanContext::new(
            self.trace_id,
            self.span_id,
            trace_flags,
            true,
            TraceState::default(),
        );
        let parent_context = Context::new().with_remote_span_context(span_context);
        let _ = span.set_parent(parent_context);
        self.trace_id.to_string()
    }
}

impl Serialize for SpanContext {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("SpanContext", 3)?;
        state.serialize_field("trace_id", &self.trace_id.to_string())?;
        state.serialize_field("span_id", &self.span_id.to_string())?;
        state.serialize_field("sampled", &self.sampled)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for SpanContext {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Fields {
            trace_id: String,
            span_id: String,
            sampled: bool,
        }

        let fields = Fields::deserialize(deserializer)?;
        let trace_id = TraceId::from_hex(&fields.trace_id).map_err(D::Error::custom)?;
        if trace_id == TraceId::INVALID {
            return Err(D::Error::custom("trace_id must be a non-zero hex value"));
        }

        let span_id = SpanId::from_hex(&fields.span_id).map_err(D::Error::custom)?;
        if span_id == SpanId::INVALID {
            return Err(D::Error::custom("span_id must be a non-zero hex value"));
        }

        Ok(Self {
            trace_id,
            span_id,
            sampled: fields.sampled,
        })
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry::trace::{SpanId, TraceId};
    use serde_json::json;

    use super::SpanContext;

    fn build_span_context() -> SpanContext {
        SpanContext {
            trace_id: TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736")
                .expect("trace ID should parse"),
            span_id: SpanId::from_hex("00f067aa0ba902b7").expect("span ID should parse"),
            sampled: true,
        }
    }

    #[test]
    fn serde_roundtrip() {
        let span_context = build_span_context();

        let serialized = serde_json::to_value(&span_context).expect("should serialize");
        assert_eq!(
            serialized,
            json!({
                "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
                "span_id": "00f067aa0ba902b7",
                "sampled": true,
            })
        );

        let deserialized: SpanContext =
            serde_json::from_value(serialized).expect("should deserialize");
        assert_eq!(deserialized, span_context);
    }

    #[test]
    fn deserialize_invalid_hex_rejected() {
        let input = json!({
            "trace_id": "not_hex",
            "span_id": "00f067aa0ba902b7",
            "sampled": true,
        });

        let result = serde_json::from_value::<SpanContext>(input);
        assert!(result.is_err());
    }

    #[test]
    fn deserialize_zero_trace_id_rejected() {
        let input = json!({
            "trace_id": "00000000000000000000000000000000",
            "span_id": "00f067aa0ba902b7",
            "sampled": true,
        });

        let result = serde_json::from_value::<SpanContext>(input);
        assert!(result.is_err());
    }

    #[test]
    fn deserialize_zero_span_id_rejected() {
        let input = json!({
            "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
            "span_id": "0000000000000000",
            "sampled": true,
        });

        let result = serde_json::from_value::<SpanContext>(input);
        assert!(result.is_err());
    }

    #[test]
    fn set_parent_returns_trace_id() {
        let span_context = build_span_context();
        let span = tracing::info_span!("test-span");
        assert_eq!(
            span_context.set_parent(&span),
            "4bf92f3577b34da6a3ce929d0e0e4736"
        );
    }
}
