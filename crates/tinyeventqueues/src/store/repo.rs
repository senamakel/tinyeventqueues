//! Small typed CAS helper over the v2 document port.

use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::Result;
use crate::ports::{DocumentStore, ErrorKind, Precondition, Query, Version, Versioned};

const UPDATE_ATTEMPTS: u32 = 32;

/// A typed collection handle for queue-owned document records.
#[derive(Clone)]
pub(crate) struct Repo<T> {
    docs: Arc<dyn DocumentStore>,
    collection: &'static str,
    marker: std::marker::PhantomData<fn() -> T>,
}

impl<T> std::fmt::Debug for Repo<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Repo")
            .field("collection", &self.collection)
            .finish_non_exhaustive()
    }
}

impl<T> Repo<T> {
    pub(crate) fn new(docs: &Arc<dyn DocumentStore>, collection: &'static str) -> Self {
        Self {
            docs: Arc::clone(docs),
            collection,
            marker: std::marker::PhantomData,
        }
    }
}

impl<T: Serialize + DeserializeOwned + Send + Sync> Repo<T> {
    pub(crate) async fn get(&self, id: &str) -> Result<Option<Versioned<T>>> {
        let Some(found) = self.docs.get(self.collection, id).await? else {
            return Ok(None);
        };
        Ok(Some(Versioned {
            id: found.id,
            version: found.version,
            doc: serde_json::from_value(found.doc)?,
            meta: found.meta,
        }))
    }

    pub(crate) async fn query(
        &self,
        query: &Query,
    ) -> Result<tinystoragedrivers_types::Page<Versioned<T>>> {
        let page = self.docs.query(self.collection, query).await?;
        Ok(tinystoragedrivers_types::Page {
            items: page
                .items
                .into_iter()
                .map(|found| {
                    Ok(Versioned {
                        id: found.id,
                        version: found.version,
                        doc: serde_json::from_value(found.doc)?,
                        meta: found.meta,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            next: page.next,
        })
    }

    pub(crate) async fn update<F>(&self, id: &str, mut transform: F) -> Result<Option<(T, Version)>>
    where
        F: FnMut(Option<T>) -> Result<Option<T>> + Send,
    {
        for attempt in 0..UPDATE_ATTEMPTS {
            let (precondition, current) = match self.docs.get(self.collection, id).await? {
                Some(found) => {
                    let current = serde_json::from_value(found.doc)?;
                    (Precondition::Version(found.version), Some(current))
                }
                None => (Precondition::Absent, None),
            };
            let Some(next) = transform(current)? else {
                return Ok(None);
            };
            let body = serde_json::to_value(&next)?;
            match self.docs.put(self.collection, id, body, precondition).await {
                Ok(version) => return Ok(Some((next, version))),
                Err(error)
                    if error.kind() == ErrorKind::Conflict && attempt + 1 < UPDATE_ATTEMPTS =>
                {
                    let millis = 1_u64 << attempt.min(3);
                    tokio::time::sleep(Duration::from_millis(millis)).await;
                }
                Err(error) => return Err(error.into()),
            }
        }
        Err(tinystoragedrivers_types::StorageError::conflict(
            "document update exceeded its CAS retry budget",
        )
        .into())
    }
}

#[cfg(test)]
#[path = "repo_tests.rs"]
mod tests;
