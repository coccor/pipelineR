//! Conversion functions between core record/value types and protobuf types.

use crate::record::{Record as CoreRecord, RecordBatch as CoreBatch, Value as CoreValue};
use chrono::TimeZone;
use pipeliner_proto::pipeliner::v1 as proto;
use std::collections::HashMap;

/// Convert a core `Value` to its protobuf representation.
pub fn core_value_to_proto(val: &CoreValue) -> proto::Value {
    let kind = match val {
        CoreValue::Null => Some(proto::value::Kind::NullValue(true)),
        CoreValue::Bool(b) => Some(proto::value::Kind::BoolValue(*b)),
        CoreValue::Int(i) => Some(proto::value::Kind::IntValue(*i)),
        CoreValue::Float(f) => Some(proto::value::Kind::FloatValue(*f)),
        CoreValue::String(s) => Some(proto::value::Kind::StringValue(s.clone())),
        CoreValue::Bytes(b) => Some(proto::value::Kind::BytesValue(b.clone())),
        CoreValue::Timestamp(ts) => {
            let t = prost_types::Timestamp {
                seconds: ts.timestamp(),
                nanos: ts.timestamp_subsec_nanos() as i32,
            };
            Some(proto::value::Kind::TimestampValue(t))
        }
        CoreValue::Array(arr) => {
            let values = arr.iter().map(core_value_to_proto).collect();
            Some(proto::value::Kind::ArrayValue(proto::ArrayValue { values }))
        }
        CoreValue::Map(m) => {
            let fields: HashMap<String, proto::Value> = m
                .iter()
                .map(|(k, v)| (k.clone(), core_value_to_proto(v)))
                .collect();
            Some(proto::value::Kind::MapValue(proto::MapValue { fields }))
        }
    };
    proto::Value { kind }
}

/// Convert a protobuf `Value` to its core representation.
///
/// Returns an error if a timestamp value cannot be converted.
pub fn proto_value_to_core(val: &proto::Value) -> Result<CoreValue, String> {
    match &val.kind {
        None => Ok(CoreValue::Null),
        Some(kind) => match kind {
            proto::value::Kind::NullValue(_) => Ok(CoreValue::Null),
            proto::value::Kind::BoolValue(b) => Ok(CoreValue::Bool(*b)),
            proto::value::Kind::IntValue(i) => Ok(CoreValue::Int(*i)),
            proto::value::Kind::FloatValue(f) => Ok(CoreValue::Float(*f)),
            proto::value::Kind::StringValue(s) => Ok(CoreValue::String(s.clone())),
            proto::value::Kind::BytesValue(b) => Ok(CoreValue::Bytes(b.clone())),
            proto::value::Kind::TimestampValue(ts) => {
                let dt = chrono::Utc
                    .timestamp_opt(ts.seconds, ts.nanos as u32)
                    .single()
                    .ok_or_else(|| {
                        format!(
                            "invalid timestamp: seconds={}, nanos={}",
                            ts.seconds, ts.nanos
                        )
                    })?;
                Ok(CoreValue::Timestamp(dt))
            }
            proto::value::Kind::ArrayValue(arr) => {
                let values: Result<Vec<CoreValue>, String> =
                    arr.values.iter().map(proto_value_to_core).collect();
                Ok(CoreValue::Array(values?))
            }
            proto::value::Kind::MapValue(m) => {
                let mut map = CoreRecord::new();
                for (k, v) in &m.fields {
                    map.insert(k.clone(), proto_value_to_core(v)?);
                }
                Ok(CoreValue::Map(map))
            }
        },
    }
}

/// Convert a core `Record` to its protobuf representation.
pub fn core_record_to_proto(rec: &CoreRecord) -> proto::Record {
    let fields: HashMap<String, proto::Value> = rec
        .iter()
        .map(|(k, v)| (k.clone(), core_value_to_proto(v)))
        .collect();
    proto::Record { fields }
}

/// Convert a protobuf `Record` to its core representation.
pub fn proto_record_to_core(rec: &proto::Record) -> CoreRecord {
    let mut core_rec = CoreRecord::new();
    for (k, v) in &rec.fields {
        let core_val = proto_value_to_core(v).unwrap_or(CoreValue::Null);
        core_rec.insert(k.clone(), core_val);
    }
    core_rec
}

/// Convert a core `RecordBatch` to its protobuf representation.
pub fn core_batch_to_proto(batch: &CoreBatch) -> proto::RecordBatch {
    let records = batch.records.iter().map(core_record_to_proto).collect();
    proto::RecordBatch { records }
}

/// Convert a protobuf `RecordBatch` to its core representation.
pub fn proto_batch_to_core(batch: &proto::RecordBatch) -> CoreBatch {
    let records = batch.records.iter().map(proto_record_to_core).collect();
    CoreBatch::new(records)
}
