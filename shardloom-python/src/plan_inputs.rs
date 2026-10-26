//! Immutable input ownership, shared independently of prepared operator state.

use super::{BatchDeclaration, CancellationToken, Result, SqlSource, SqlSourceBinding, failed};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

#[derive(Default)]
pub(super) struct InputOwners {
    bindings: Vec<SqlSourceBinding>,
    batches: Vec<BatchDeclaration>,
    parents: Vec<Arc<Self>>,
    pub(super) has_batches: bool,
}

pub(super) struct Inputs<'a> {
    pub(super) bindings: Vec<&'a SqlSourceBinding>,
    pub(super) batches: Vec<&'a BatchDeclaration>,
}

impl InputOwners {
    pub(super) fn new(
        bindings: Vec<SqlSourceBinding>,
        batches: Vec<BatchDeclaration>,
        parents: Vec<Arc<Self>>,
    ) -> Result<Arc<Self>> {
        let has_batches = !batches.is_empty() || parents.iter().any(|parent| parent.has_batches);
        let owners = Arc::new(Self {
            bindings,
            batches,
            parents,
            has_batches,
        });
        // Reject ambiguous ownership before any callback, file or memory intake.
        owners.inputs(&CancellationToken::default())?;
        Ok(owners)
    }

    pub(super) fn inputs(&self, cancellation: &CancellationToken) -> Result<Inputs<'_>> {
        let mut pending = vec![self];
        let mut seen = BTreeSet::new();
        let mut bindings = BTreeMap::<&str, &SqlSourceBinding>::new();
        let mut batches = BTreeMap::<&str, &BatchDeclaration>::new();
        while let Some(owner) = pending.pop() {
            cancellation.check()?;
            if !seen.insert(std::ptr::from_ref(owner)) {
                continue;
            }
            pending.extend(owner.parents.iter().map(Arc::as_ref));
            for binding in &owner.bindings {
                if batches.contains_key(binding.reference.as_str()) {
                    return Err(failed("source reference has conflicting input owners"));
                }
                if let Some(previous) = bindings.insert(&binding.reference, binding) {
                    let same = match (&previous.source, &binding.source) {
                        (
                            SqlSource::File {
                                format: a,
                                schema: sa,
                            },
                            SqlSource::File {
                                format: b,
                                schema: sb,
                            },
                        ) => a == b && sa == sb,
                        (SqlSource::Memory(a), SqlSource::Memory(b)) => a.same_snapshot(b),
                        (
                            SqlSource::Range {
                                column: ac,
                                start: a,
                                step: ast,
                                rows: ar,
                            },
                            SqlSource::Range {
                                column: bc,
                                start: b,
                                step: bst,
                                rows: br,
                            },
                        ) => ac == bc && a == b && ast == bst && ar == br,
                        _ => false,
                    };
                    if !same {
                        return Err(failed("source reference has conflicting input owners"));
                    }
                }
            }
            for declaration in &owner.batches {
                if bindings.contains_key(declaration.reference.as_str()) {
                    return Err(failed("source reference has conflicting input owners"));
                }
                if let Some(previous) = batches.insert(&declaration.reference, declaration)
                    && !previous.same_owner(declaration)
                {
                    return Err(failed("source reference has conflicting producer owners"));
                }
            }
        }
        Ok(Inputs {
            bindings: bindings.into_values().collect(),
            batches: batches.into_values().collect(),
        })
    }
}

impl Drop for InputOwners {
    fn drop(&mut self) {
        let mut pending = std::mem::take(&mut self.parents);
        while let Some(owner) = pending.pop() {
            if let Ok(mut owner) = Arc::try_unwrap(owner) {
                pending.append(&mut owner.parents);
            }
        }
    }
}
