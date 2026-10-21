//! Tests for the module's storage declaration.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use super::*;

#[test]
fn collection_specs_are_pure_owner_declarations() {
    let specs = collection_specs();
    assert_eq!(specs.len(), 12);
    let mut names = std::collections::HashSet::new();
    for spec in &specs {
        assert!(spec.name.starts_with("teq."), "{}", spec.name);
        assert!(
            names.insert(&spec.name),
            "duplicate collection {}",
            spec.name
        );
    }
    assert_eq!(specs[0].name, OUTCOMES);
    assert_eq!(specs[0].indexes[0].fields, ["disposition"]);
    assert_eq!(specs[0].indexes[1].fields, ["expires_ms"]);
    assert_eq!(specs.last().unwrap().name, COALESCE);
}

#[test]
fn host_can_compose_collection_specs_with_its_other_storage() {
    let mut all = collection_specs();
    all.push(CollectionSpec::new("host.preferences"));
    let mut names = std::collections::HashSet::new();
    assert!(all.iter().all(|spec| names.insert(spec.name.as_str())));
}

#[test]
fn shipped_files_are_json_documents() {
    for text in [SHIPPED_QUEUES_JSON, SHIPPED_PIPELINE_JSON] {
        let value: serde_json::Value = serde_json::from_str(text).unwrap();
        assert_eq!(value["version"], 1);
    }
}
