use criterion::{criterion_group, criterion_main, Criterion};
use takeln::{CheckpointStatus, Checkpointer, InMemoryCheckpointer};

#[derive(Clone, serde::Serialize, serde::Deserialize, Default)]
struct BenchState {
    data: String,
}

fn bench_checkpoint_save_load(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("checkpoint");

    group.bench_function("inmemory_save", |b| {
        b.iter(|| {
            rt.block_on(async {
                let cp = InMemoryCheckpointer::<BenchState>::new();
                let state = BenchState { data: "x".repeat(1024) };
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
                .unwrap()
            })
        })
    });

    group.bench_function("inmemory_save_load_cycle", |b| {
        b.iter(|| {
            rt.block_on(async {
                let cp = InMemoryCheckpointer::<BenchState>::new();
                let state = BenchState { data: "x".repeat(1024) };
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

    group.finish();
}

criterion_group!(benches, bench_checkpoint_save_load);
criterion_main!(benches);
