//! Every type without its own module (Array, Map, Decimal, …): values pass through as the
//! JSON they arrived as, and queries can only select them.

use serde_json::Value as Json;

use super::{Class, ColumnType, Literal, LiteralError, Ops, Type, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OtherType;

impl ColumnType for OtherType {
    fn decode(&self, json: &Json) -> Option<Value> {
        Some(Value::Raw(json.clone()))
    }

    /// Never reached: the binder doesn't let these columns be compared.
    fn literal(&self, _: &Literal, _: &Type) -> Result<Value, LiteralError> {
        Err(LiteralError::Mismatch)
    }

    fn zero(&self) -> Value {
        Value::Raw(Json::Null)
    }

    fn class(&self) -> Option<Class> {
        None
    }

    fn ops(&self) -> Ops {
        Ops::NONE
    }
}
