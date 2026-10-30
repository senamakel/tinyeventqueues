//! Tests for the module configuration blob.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use serde_json::{Value, json};

use super::*;
use crate::config::{SHIPPED_PIPELINE_JSON, SHIPPED_QUEUES_JSON};

#[test]
fn null_and_an_empty_object_run_the_shipped_files() {
    for blob in [Value::Null, json!({})] {
        let settings = Settings::parse(blob).unwrap();
        assert!(settings.storage_address().unwrap().is_none());
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
    let error = Settings::parse(json!({ "storage_addresses": {} })).unwrap_err();
    assert!(error.to_string().contains("storage_addresses"), "{error}");
    let error = Settings::parse(json!({ "timer": { "every": 1 } })).unwrap_err();
    assert!(error.to_string().contains("every"), "{error}");
}

#[test]
fn an_invocation_storage_address_is_validated_from_host_and_root() {
    let settings = Settings::parse(json!({
        "storage_address": {
            "host": ":1.23",
            "object_root": "/ai/tinyhumans/tinystorage/Invocation/7"
        }
    }))
    .unwrap();
    let address = settings.storage_address().unwrap().unwrap();
    assert_eq!(address.host().as_str(), ":1.23");
    assert_eq!(
        address.object_root().as_str(),
        "/ai/tinyhumans/tinystorage/Invocation/7"
    );
}

#[test]
fn storage_addresses_reject_non_unique_hosts_and_unscoped_roots() {
    for config in [
        json!({ "host": "ai.tinyhumans.host", "object_root": "/ai/tinyhumans/tinystorage/Invocation/7" }),
        json!({ "host": ":1.23", "object_root": "/ai/tinyhumans/tinystorage/Leases/7" }),
    ] {
        let settings = Settings::parse(json!({ "storage_address": config })).unwrap();
        assert!(settings.storage_address().is_err());
    }
}

#[test]
fn an_invalid_object_path_is_reported_as_a_configuration_error() {
    let settings = Settings::parse(json!({
        "storage_address": {
            "host": ":1.23",
            "object_root": "not/an/object/path"
        }
    }))
    .unwrap();

    let error = settings.storage_address().unwrap_err();
    assert!(
        error.to_string().contains("storage_address.object_root"),
        "{error}"
    );
}

#[test]
fn old_lease_descriptor_fields_are_not_accepted_as_an_address() {
    let error = Settings::parse(json!({
        "storage_descriptor": {
            "service_peer": ":1.23",
            "registration_id": 7,
            "object_root": "/ai/tinyhumans/tinystorage/Leases/7"
        }
    }))
    .unwrap_err();
    assert!(error.to_string().contains("storage_descriptor"), "{error}");
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

#[test]
fn a_host_bound_storage_callback_is_an_accepted_module_configuration() {
    let settings = Settings::parse(json!({"storage_callback": ":1.23"}));
    assert!(
        settings.is_ok(),
        "host callback config was refused: {settings:?}"
    );
}

#[test]
fn storage_callbacks_require_a_unique_peer_and_one_route() {
    let settings = Settings::parse(json!({"storage_callback": ":1.23"})).unwrap();
    assert_eq!(
        settings.storage_callback().unwrap().unwrap().as_str(),
        ":1.23"
    );
    for peer in ["ai.tinyhumans.tinystorage.Storage", "not a peer", ""] {
        let settings = Settings::parse(json!({"storage_callback": peer})).unwrap();
        assert!(settings.storage_callback().is_err());
    }
    let settings = Settings::parse(json!({
        "storage_callback": ":1.23",
        "storage_address": {"host": ":1.24", "object_root": "/ai/tinyhumans/tinystorage/Invocation/x"}
    })).unwrap();
    assert!(settings.storage_callback().is_err());
}
