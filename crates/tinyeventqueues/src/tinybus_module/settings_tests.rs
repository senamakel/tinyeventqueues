//! Tests for the module configuration blob.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use serde_json::{Value, json};

use super::*;
use crate::config::{SHIPPED_PIPELINE_JSON, SHIPPED_QUEUES_JSON};

#[test]
fn null_and_an_empty_object_run_the_shipped_files() {
    for blob in [Value::Null, json!({})] {
        let settings = Settings::parse(blob).unwrap();
        assert!(settings.storage_descriptor.is_none());
        assert_eq!(settings.timer, TimerSettings::default());
        assert_eq!(settings.manifest().unwrap(), shipped_manifest().unwrap());
        assert_eq!(
            settings.pipeline().unwrap(),
            shipped_pipeline_config().unwrap()
        );
    }
}

#[test]
fn supplied_documents_replace_the_shipped_ones() {
    let mut queues: Value = serde_json::from_str(SHIPPED_QUEUES_JSON).unwrap();
    queues["queues"][0]["max_attempts"] = json!(3);
    let mut pipeline: Value = serde_json::from_str(SHIPPED_PIPELINE_JSON).unwrap();
    pipeline["ingress"]["batch_size"] = json!(7);
    let settings = Settings::parse(json!({
        "queues": queues,
        "pipeline": pipeline,
        "timer": { "enabled": false, "min_interval_ms": 10, "max_interval_ms": 20 },
    }))
    .unwrap();
    assert_eq!(settings.manifest().unwrap().queues[0].max_attempts, 3);
    assert_eq!(settings.pipeline().unwrap().ingress.batch_size, 7);
    assert!(!settings.timer.enabled);
    assert_eq!(settings.timer.max_interval_ms, 20);
}

#[test]
fn an_unknown_field_is_named() {
    let error = Settings::parse(json!({ "timers": {} })).unwrap_err();
    assert!(error.to_string().contains("timers"), "{error}");
    let error = Settings::parse(json!({ "storage_descriptors": {} })).unwrap_err();
    assert!(error.to_string().contains("storage_descriptors"), "{error}");
    let error = Settings::parse(json!({ "timer": { "every": 1 } })).unwrap_err();
    assert!(error.to_string().contains("every"), "{error}");
}

#[test]
fn a_host_storage_descriptor_is_accepted_as_opaque_configuration() {
    let descriptor = json!({
        "service_peer": ":1.23",
        "registration_id": 7,
        "object_root": "/ai/tinyhumans/tinystorage/Leases/7",
    });
    let settings = Settings::parse(json!({ "storage_descriptor": descriptor })).unwrap();
    let descriptor = settings.storage_descriptor.expect("descriptor is retained");
    assert_eq!(descriptor.service_peer(), ":1.23");
    assert_eq!(
        descriptor.object_root(),
        "/ai/tinyhumans/tinystorage/Leases/7"
    );
}

#[test]
fn inverted_or_zero_timer_bounds_are_refused() {
    for timer in [
        json!({ "min_interval_ms": 0 }),
        json!({ "min_interval_ms": 10, "max_interval_ms": 5 }),
    ] {
        let error = Settings::parse(json!({ "timer": timer })).unwrap_err();
        assert!(error.to_string().contains("min_interval_ms"), "{error}");
    }
}

#[test]
fn an_invalid_document_fails_when_it_is_read() {
    let settings = Settings::parse(json!({
        "queues": { "version": 1, "queues": [{ "name": "" }] },
        "pipeline": { "version": 99 },
    }))
    .unwrap();
    assert!(settings.manifest().is_err());
    assert!(settings.pipeline().is_err());
}
