use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Status of a node within a DAG execution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum NodeStatus {
    /// Node has not yet been scheduled.
    Pending,
    /// Node is currently executing.
    Running,
    /// Node completed successfully.
    Done,
    /// Node execution failed.
    Failed,
    /// Node yielded (suspended for human-in-the-loop or external input).
    Yielded,
}

/// A single node within a DAG, representing one unit of work.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DAGNode {
    /// Unique identifier for this node.
    pub id: Uuid,
    /// The registered step type name that maps to a `Node<S>` in the `Graph`.
    pub step_type: String,
    /// IDs of nodes that must complete before this node can execute.
    pub depends_on: Vec<Uuid>,
    /// Current execution status.
    pub status: NodeStatus,
}

/// A Directed Acyclic Graph defining the execution topology for parallel wave scheduling.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DAG {
    /// Unique identifier for this DAG instance.
    pub id: Uuid,
    /// The ordered list of nodes in this DAG.
    pub nodes: Vec<DAGNode>,
    /// Timestamp when this DAG was created.
    pub created_at: DateTime<Utc>,
}

impl DAG {
    /// Create a new empty DAG.
    pub fn new() -> Self {
        Self {
            id: Uuid::new_v4(),
            nodes: Vec::new(),
            created_at: Utc::now(),
        }
    }

    /// Add a node to the DAG with the given step type and dependencies.
    /// Returns the auto-generated UUID for the new node.
    pub fn add_node(&mut self, step_type: impl Into<String>, depends_on: Vec<Uuid>) -> Uuid {
        let id = Uuid::new_v4();
        self.nodes.push(DAGNode {
            id,
            step_type: step_type.into(),
            depends_on,
            status: NodeStatus::Pending,
        });
        id
    }

    /// Restore node statuses from a previously checkpointed DAG snapshot.
    /// Matches nodes by their UUID and updates the status accordingly.
    pub fn restore_statuses(&mut self, checkpointed: &DAG) {
        let status_map: std::collections::HashMap<Uuid, NodeStatus> =
            checkpointed.nodes.iter().map(|n| (n.id, n.status.clone())).collect();

        for node in &mut self.nodes {
            if let Some(status) = status_map.get(&node.id) {
                node.status = status.clone();
            }
        }
    }
}

impl Default for DAG {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for constructing DAGs with string-based dependency references.
///
/// # Example
/// ```rust
/// use takeln::DAG;
/// let dag = DAG::builder()
///     .node("fetch", &[])
///     .node("parse", &["fetch"])
///     .node("score", &["parse"])
///     .node("rank", &["parse"])
///     .node("merge", &["score", "rank"])
///     .build()
///     .unwrap();
/// ```
///
/// # Constraining step types
///
/// By default any step type is accepted, which is right for an engine that does not
/// know what its nodes mean. A caller that *does* have a declared set of valid step
/// types can hand it over with [`DAGBuilder::allow_step_types`], and a node outside
/// it then fails the build:
///
/// ```rust
/// use takeln::DAG;
/// let err = DAG::builder()
///     .allow_step_types(["fetch", "parse"])
///     .node("fetch", &[])
///     .node("parce", &["fetch"])   // typo
///     .build()
///     .unwrap_err();
/// assert!(err.contains("parce"));
/// ```
///
/// This exists because an unconstrained step type is a silent failure downstream:
/// anything that keys per-node state, memory or metrics off the name gets a fresh
/// bucket for the typo, and every read goes to the correctly spelled one. Nothing
/// goes red; the data just quietly splits. A build error trades that for one loud
/// failure, once.
pub struct DAGBuilder {
    entries: Vec<(String, Vec<String>)>,
    /// `None` means unconstrained. An empty set means nothing is allowed, which is
    /// deliberately distinct: a caller that declared no step types wants the build
    /// to fail, not to be waved through.
    allowed_step_types: Option<std::collections::BTreeSet<String>>,
}

impl DAGBuilder {
    /// Restrict which step types [`DAGBuilder::build`] will accept.
    ///
    /// Calling it replaces any previous set. Passing an empty iterator allows
    /// nothing: "I declared no step types" is a caller error worth surfacing, not a
    /// licence to accept everything.
    pub fn allow_step_types<I, S>(mut self, step_types: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.allowed_step_types = Some(step_types.into_iter().map(Into::into).collect());
        self
    }

    /// Add a node with the given step type and dependency names.
    pub fn node(mut self, step_type: &str, depends_on: &[&str]) -> Self {
        self.entries.push((
            step_type.to_string(),
            depends_on.iter().map(|s| s.to_string()).collect(),
        ));
        self
    }

    /// Build the DAG, resolving string dependencies to UUIDs.
    ///
    /// Returns an error if any dependency references a node that doesn't exist,
    /// or if a cycle is detected.
    pub fn build(self) -> Result<DAG, String> {
        // Before dependency resolution: an unregistered step type is a mistake in
        // the DAG's vocabulary, and reporting it as a missing dependency (which it
        // also usually is) sends the reader to the wrong problem. Every offender is
        // named at once so a multi-typo DAG takes one build to fix, not one each.
        if let Some(allowed) = &self.allowed_step_types {
            let offenders: Vec<&str> = self
                .entries
                .iter()
                .map(|(step_type, _)| step_type.as_str())
                .filter(|step_type| !allowed.contains(*step_type))
                .collect();
            if !offenders.is_empty() {
                let declared = if allowed.is_empty() {
                    "none declared".to_string()
                } else {
                    allowed.iter().cloned().collect::<Vec<_>>().join(", ")
                };
                return Err(format!(
                    "step type(s) not allowed: {} (allowed: {})",
                    offenders.join(", "),
                    declared
                ));
            }
        }

        let mut dag = DAG::new();
        let mut name_to_id: std::collections::HashMap<String, Uuid> = std::collections::HashMap::new();

        // First pass: create all node IDs
        for (step_type, _) in &self.entries {
            let id = Uuid::new_v4();
            name_to_id.insert(step_type.clone(), id);
        }

        // Second pass: resolve deps and build nodes
        for (step_type, deps) in &self.entries {
            let mut resolved_deps = Vec::new();
            for dep in deps {
                match name_to_id.get(dep) {
                    Some(id) => resolved_deps.push(*id),
                    None => return Err(format!("Dependency '{}' not found for node '{}'", dep, step_type)),
                }
            }
            let id = name_to_id[step_type];
            dag.nodes.push(DAGNode {
                id,
                step_type: step_type.clone(),
                depends_on: resolved_deps,
                status: NodeStatus::Pending,
            });
        }

        // Cycle detection via topological sort
        let n = dag.nodes.len();
        let _id_to_idx: std::collections::HashMap<Uuid, usize> =
            dag.nodes.iter().enumerate().map(|(i, node)| (node.id, i)).collect();
        let mut in_degree = vec![0usize; n];
        for (i, node) in dag.nodes.iter().enumerate() {
            in_degree[i] = node.depends_on.len();
        }
        let mut queue: std::collections::VecDeque<usize> = in_degree
            .iter()
            .enumerate()
            .filter(|(_, &d)| d == 0)
            .map(|(i, _)| i)
            .collect();
        let mut visited = 0usize;
        while let Some(idx) = queue.pop_front() {
            visited += 1;
            let node_id = dag.nodes[idx].id;
            for (i, node) in dag.nodes.iter().enumerate() {
                if node.depends_on.contains(&node_id) {
                    in_degree[i] -= 1;
                    if in_degree[i] == 0 {
                        queue.push_back(i);
                    }
                }
            }
        }
        if visited != n {
            return Err("Cycle detected in DAG".to_string());
        }

        Ok(dag)
    }
}

impl DAG {
    /// Create a builder for constructing a DAG with string-based dependency references.
    pub fn builder() -> DAGBuilder {
        DAGBuilder {
            entries: Vec::new(),
            allowed_step_types: None,
        }
    }
}

#[cfg(test)]
mod step_type_allowlist_tests {
    use super::*;

    #[test]
    fn an_unconstrained_builder_still_accepts_anything() {
        // The default must not change: takeln is a general engine and most callers
        // have no declared vocabulary to check against.
        let dag = DAG::builder()
            .node("whatever", &[])
            .node("anything", &["whatever"])
            .build()
            .unwrap();
        assert_eq!(dag.nodes.len(), 2);
    }

    #[test]
    fn a_declared_step_type_builds() {
        let dag = DAG::builder()
            .allow_step_types(["fetch", "parse"])
            .node("fetch", &[])
            .node("parse", &["fetch"])
            .build()
            .unwrap();
        assert_eq!(dag.nodes.len(), 2);
    }

    #[test]
    fn an_undeclared_step_type_fails_the_build() {
        let error = DAG::builder()
            .allow_step_types(["fetch", "parse"])
            .node("fetch", &[])
            .node("score", &["fetch"])
            .build()
            .unwrap_err();
        assert!(error.contains("score"), "{error}");
        assert!(
            error.contains("fetch") && error.contains("parse"),
            "the error must name what IS allowed, or it is not actionable: {error}"
        );
    }

    #[test]
    fn every_offender_is_reported_in_one_build() {
        let error = DAG::builder()
            .allow_step_types(["fetch"])
            .node("fetch", &[])
            .node("scoer", &["fetch"])
            .node("rnak", &["fetch"])
            .build()
            .unwrap_err();
        assert!(error.contains("scoer") && error.contains("rnak"), "{error}");
    }

    #[test]
    fn an_empty_allowlist_allows_nothing() {
        let error = DAG::builder()
            .allow_step_types(Vec::<String>::new())
            .node("fetch", &[])
            .build()
            .unwrap_err();
        assert!(error.contains("none declared"), "{error}");
    }

    #[test]
    fn the_allowlist_is_checked_before_dependency_resolution() {
        // Both faults are present. The vocabulary error is the one to report: a
        // misspelled node is usually ALSO a missing dependency, and naming the
        // dependency sends the reader hunting for a node that was never meant to
        // exist under that name.
        let error = DAG::builder()
            .allow_step_types(["fetch"])
            .node("scoer", &["nonexistent"])
            .build()
            .unwrap_err();
        assert!(error.contains("not allowed"), "{error}");
    }
}
