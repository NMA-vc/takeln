/// Trait for merging parallel execution results into a single state.
///
/// When multiple DAG nodes execute in the same wave, their output states
/// are merged sequentially in deterministic DAG node index order.
///
/// # Example
/// ```rust
/// use takeln::Merge;
///
/// #[derive(Clone, Default)]
/// struct MyState { values: Vec<String> }
///
/// impl Merge for MyState {
///     fn merge(&mut self, other: Self) {
///         self.values.extend(other.values);
///     }
/// }
/// ```
pub trait Merge {
    /// Merge `other` into `self`. Called sequentially in DAG node index order
    /// when parallel wave results are combined.
    fn merge(&mut self, other: Self);
}
