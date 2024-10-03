// This is copied verbatim from this comment: https://github.com/launchbadge/sqlx/issues/3487#issuecomment-2366739587
//
// It seems the intention is to roll this into the `jiff` project as a e.g.
// `jiff-sqlx` crate, at which point this entire module should be deleted and
// replaced with that.

use std::str::FromStr;

use jiff::SignedDuration;
use serde::{Deserialize, Serialize};
use sqlx::{
    encode::IsNull,
    error::BoxDynError,
    postgres::{types::Oid, PgArgumentBuffer, PgHasArrayType, PgTypeInfo, PgValueFormat},
    Database, Decode, Encode, Postgres, Type,
};

/// A module for Jiff support of SQLx.

// TODO(tisonkun): either switch to the upstream [1] or spawn a dedicate open-source crate.
// [1] https://github.com/launchbadge/sqlx/pull/3511
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Timestamp(pub jiff::Timestamp);

impl Type<Postgres> for Timestamp {
    fn type_info() -> PgTypeInfo {
        // 1184 => PgType::Timestamptz
        PgTypeInfo::with_oid(Oid(1184))
    }
}

impl PgHasArrayType for Timestamp {
    fn array_type_info() -> PgTypeInfo {
        // 1185 => PgType::TimestamptzArray
        PgTypeInfo::with_oid(Oid(1185))
    }
}

impl Encode<'_, Postgres> for Timestamp {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
        // TIMESTAMP is encoded as the microseconds since the epoch
        let micros = self
            .0
            .duration_since(postgres_epoch_timestamp())
            .as_micros();
        let micros = i64::try_from(micros)
            .map_err(|_| format!("Timestamp {} out of range for Postgres: {micros}", self.0))?;
        Encode::<Postgres>::encode(micros, buf)
    }

    fn size_hint(&self) -> usize {
        size_of::<i64>()
    }
}

impl<'r> Decode<'r, Postgres> for Timestamp {
    fn decode(value: <Postgres as Database>::ValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(match value.format() {
            PgValueFormat::Binary => {
                // TIMESTAMP is encoded as the microseconds since the epoch
                let us = Decode::<Postgres>::decode(value)?;
                let ts = postgres_epoch_timestamp().checked_add(SignedDuration::from_micros(us))?;
                Timestamp(ts)
            }
            PgValueFormat::Text => {
                let s = value.as_str()?;
                let ts = jiff::Timestamp::from_str(s)?;
                Timestamp(ts)
            }
        })
    }
}

fn postgres_epoch_timestamp() -> jiff::Timestamp {
    jiff::Timestamp::from_str("2000-01-01T00:00:00Z")
        .expect("2000-01-01T00:00:00Z is a valid timestamp")
}
