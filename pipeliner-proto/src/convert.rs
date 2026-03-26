//! Conversion functions between `pipeliner_core` record/value types and protobuf types.

use crate::pipeliner::v1 as proto;
use chrono::TimeZone;
use pipeliner_core::record::{
    Record as CoreRecord, RecordBatch as CoreBatch, Value as CoreValue,
};
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
        // proto_value_to_core only fails on invalid timestamps; default to Null on error.
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn convert_null() {
        let proto_val = core_value_to_proto(&CoreValue::Null);
        let round = proto_value_to_core(&proto_val).unwrap();
        assert_eq!(round, CoreValue::Null);
    }

    #[test]
    fn convert_bool() {
        let proto_val = core_value_to_proto(&CoreValue::Bool(true));
        let round = proto_value_to_core(&proto_val).unwrap();
        assert_eq!(round, CoreValue::Bool(true));
    }

    #[test]
    fn convert_int() {
        let proto_val = core_value_to_proto(&CoreValue::Int(42));
        let round = proto_value_to_core(&proto_val).unwrap();
        assert_eq!(round, CoreValue::Int(42));
    }

    #[test]
    fn convert_float() {
        let proto_val = core_value_to_proto(&CoreValue::Float(3.14));
        let round = proto_value_to_core(&proto_val).unwrap();
        assert_eq!(round, CoreValue::Float(3.14));
    }

    #[test]
    fn convert_string() {
        let proto_val = core_value_to_proto(&CoreValue::String("hello".into()));
        let round = proto_value_to_core(&proto_val).unwrap();
        assert_eq!(round, CoreValue::String("hello".into()));
    }

    #[test]
    fn convert_bytes() {
        let proto_val = core_value_to_proto(&CoreValue::Bytes(vec![1, 2, 3]));
        let round = proto_value_to_core(&proto_val).unwrap();
        assert_eq!(round, CoreValue::Bytes(vec![1, 2, 3]));
    }

    #[test]
    fn convert_timestamp() {
        let ts = Utc::now();
        let proto_val = core_value_to_proto(&CoreValue::Timestamp(ts));
        let round = proto_value_to_core(&proto_val).unwrap();
        assert_eq!(round, CoreValue::Timestamp(ts));
    }

    #[test]
    fn convert_array() {
        let arr = CoreValue::Array(vec![CoreValue::Int(1), CoreValue::String("two".into())]);
        let proto_val = core_value_to_proto(&arr);
        let round = proto_value_to_core(&proto_val).unwrap();
        assert_eq!(round, arr);
    }

    #[test]
    fn convert_map() {
        let mut map = CoreRecord::new();
        map.insert("key".into(), CoreValue::Bool(false));
        let val = CoreValue::Map(map.clone());
        let proto_val = core_value_to_proto(&val);
        let round = proto_value_to_core(&proto_val).unwrap();
        // Map roundtrip: compare inner fields
        if let CoreValue::Map(m) = round {
            assert_eq!(m.get("key"), Some(&CoreValue::Bool(false)));
        } else {
            panic!("expected Map");
        }
    }

    #[test]
    fn convert_record_roundtrip() {
        let mut rec = CoreRecord::new();
        rec.insert("name".into(), CoreValue::String("Alice".into()));
        rec.insert("age".into(), CoreValue::Int(30));
        rec.insert("active".into(), CoreValue::Bool(true));

        let proto_rec = core_record_to_proto(&rec);
        let round = proto_record_to_core(&proto_rec);

        // Compare field-by-field since protobuf map doesn't preserve order.
        for (k, v) in &rec {
            assert_eq!(round.get(k), Some(v), "mismatch for field {k}");
        }
        assert_eq!(rec.len(), round.len());
    }

    #[test]
    fn convert_record_batch_roundtrip() {
        let mut rec1 = CoreRecord::new();
        rec1.insert("id".into(), CoreValue::Int(1));
        let mut rec2 = CoreRecord::new();
        rec2.insert("id".into(), CoreValue::Int(2));

        let batch = CoreBatch::new(vec![rec1.clone(), rec2.clone()]);
        let proto_batch = core_batch_to_proto(&batch);
        let round = proto_batch_to_core(&proto_batch);

        assert_eq!(round.records.len(), 2);
        assert_eq!(
            round.records[0].get("id"),
            Some(&CoreValue::Int(1))
        );
        assert_eq!(
            round.records[1].get("id"),
            Some(&CoreValue::Int(2))
        );
    }
}
