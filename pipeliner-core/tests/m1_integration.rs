use indexmap::IndexMap;
use pipeliner_core::dsl::parser::parse_step;
use pipeliner_core::dsl::step::execute_step;
use pipeliner_core::record::{Record, Value};

/// Build a [`Record`] from a slice of key-value pairs.
fn make_record(pairs: &[(&str, Value)]) -> Record {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

/// Simulates the "clean_transactions" example from the PRD.
///
/// Exercises rename, set with to_float, set with null-coalesce, where filter,
/// and remove across a two-record batch.
#[test]
fn clean_transactions_pipeline() {
    let steps_src = [
        "rename(.transaction_date, .txn_date)",
        "rename(.transaction_amount, .amount)",
        "set(.amount, to_float(.amount))",
        "set(.currency, .currency ?? \"USD\")",
        "where(.amount > 0.0)",
        "remove(.raw_payload)",
    ];
    let steps: Vec<_> = steps_src
        .iter()
        .map(|s| parse_step(s).expect("parse failed"))
        .collect();

    // Build metadata.merchant.name = "Acme Corp"
    let mut metadata_merchant = IndexMap::new();
    metadata_merchant.insert("name".to_string(), Value::String("Acme Corp".to_string()));
    let mut metadata = IndexMap::new();
    metadata.insert("merchant".to_string(), Value::Map(metadata_merchant));

    let mut records = vec![
        make_record(&[
            ("transaction_date", Value::String("2026-03-25".to_string())),
            ("transaction_amount", Value::String("42.50".to_string())),
            ("currency", Value::String("USD".to_string())),
            ("metadata", Value::Map(metadata.clone())),
            ("raw_payload", Value::String("{...}".to_string())),
        ]),
        make_record(&[
            ("transaction_date", Value::String("2026-03-24".to_string())),
            ("transaction_amount", Value::String("-10.00".to_string())),
            ("currency", Value::Null),
            ("metadata", Value::Map(metadata)),
            ("raw_payload", Value::String("{...}".to_string())),
        ]),
    ];

    for step in &steps {
        execute_step(step, &mut records).unwrap();
    }

    // Second record filtered by where(.amount > 0.0)
    assert_eq!(records.len(), 1);
    let rec = &records[0];
    assert_eq!(
        rec.get("txn_date"),
        Some(&Value::String("2026-03-25".to_string()))
    );
    assert!(rec.get("transaction_date").is_none());
    assert_eq!(rec.get("amount"), Some(&Value::Float(42.5)));
    assert!(rec.get("transaction_amount").is_none());
    assert_eq!(rec.get("currency"), Some(&Value::String("USD".to_string())));
    assert!(rec.get("raw_payload").is_none());
}

/// Exercises if/else expression evaluation inside a `set` step.
#[test]
fn if_else_transform() {
    let step = parse_step(
        "set(.amount_usd, if .currency == \"USD\" { .amount } else { .amount * .rate })",
    )
    .unwrap();

    let mut records = vec![
        make_record(&[
            ("amount", Value::Float(100.0)),
            ("currency", Value::String("USD".to_string())),
            ("rate", Value::Float(1.0)),
        ]),
        make_record(&[
            ("amount", Value::Float(100.0)),
            ("currency", Value::String("EUR".to_string())),
            ("rate", Value::Float(1.1)),
        ]),
    ];

    execute_step(&step, &mut records).unwrap();
    assert_eq!(records[0].get("amount_usd"), Some(&Value::Float(100.0)));
    // 100.0 * 1.1 = 110.00000000000001 due to floating point, so use approximate comparison
    match records[1].get("amount_usd") {
        Some(Value::Float(f)) => assert!((*f - 110.0).abs() < 1e-10),
        other => panic!("expected Float near 110.0, got {other:?}"),
    }
}
