use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use takeln::{CheckpointStatus, Checkpointer, InMemoryCheckpointer};

#[derive(Clone, serde::Serialize, serde::Deserialize, Default)]
struct SizedState {
    data: String,
}

fn bench_large_state(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("large_state");

    for size in [1_024, 10_240, 102_400, 1_048_576] {
        let label = match size {
            1_024 => "1KB",
            10_240 => "10KB",
            102_400 => "100KB",
            1_048_576 => "1MB",
            _ => unreachable!(),
        };

        group.bench_with_input(BenchmarkId::new("clone", label), &size, |b, &size| {
            let state = SizedState { data: "x".repeat(size) };
            b.iter(|| state.clone())
        });

        group.bench_with_input(BenchmarkId::new("serialize", label), &size, |b, &size| {
            let state = SizedState { data: "x".repeat(size) };
            b.iter(|| serde_json::to_string(&state).unwrap())
        });

        group.bench_with_input(BenchmarkId::new("checkpoint_cycle", label), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let cp = InMemoryCheckpointer::<SizedState>::new();
                    let state = SizedState { data: "x".repeat(size) };
                    cp.save_state(
                        "t1".to_string(),
                        state,
                        "next".to_string(),
                        None,
                        CheckpointStatus::Complete,
                        None,
                        None,
                        None,
                    )
                    .await
                    .unwrap();
                    cp.load_state("t1".to_string()).await.unwrap()
                })
            })
        });
    }

    group.finish();
}

criterion_group!(benches, bench_large_state);
criterion_main!(benches);
