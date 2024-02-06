use criterion::{criterion_group, criterion_main, Criterion};
use delay::prelude::{time, TaskBuilder, TaskManager};
use std::sync::Arc;
use tokio::sync::Mutex;

fn async_task_benchmark(c: &mut Criterion) {
    c.bench_function("async_task", |b| {
        b.iter(|| async {
            let manager = TaskManager::new();

            let task = TaskBuilder::default()
                .set_interval(time::Duration::from_secs(3))
                .set_process(|| async {
                    println!("Run an asynchronous task");
                })
                .build();

            manager.insert_task(task).unwrap();

            time::sleep(time::Duration::from_secs(10)).await;
        });
    });
}

fn async_task_timeout_benchmark(c: &mut Criterion) {
    c.bench_function("async_task_timeout", |b| {
        b.iter(|| async {
            let manager = TaskManager::new();

            let task = TaskBuilder::default()
                .set_timeout_from_secs(1)
                .set_process(|| async {
                    time::sleep(time::Duration::from_secs(2)).await;
                })
                .set_callback(|result| async move {
                    if let Err(e) = result {
                        println!("Task running error: {}", e)
                    };
                })
                .build();

            manager.insert_task(task).unwrap();

            time::sleep(time::Duration::from_secs(3)).await;
        });
    });
}

fn concurrent_async_task_benchmark(c: &mut Criterion) {
    c.bench_function("concurrent_async_task", |b| {
        b.iter(|| async {
            let manager = Arc::new(Mutex::new(TaskManager::new()));

            // Spawn multiple async tasks concurrently
            for _ in 0..50 {
                let manager = Arc::clone(&manager);
                tokio::spawn(async move {
                    let task = TaskBuilder::default()
                        .set_interval(time::Duration::from_secs(3))
                        .set_process(|| async {
                            println!("Run an asynchronous task");
                        })
                        .build();

                    manager.lock().await.insert_task(task).unwrap();
                });
            }

            time::sleep(time::Duration::from_secs(10)).await;
        });
    });
}

criterion_group!(benches, async_task_benchmark, async_task_timeout_benchmark, concurrent_async_task_benchmark);
criterion_main!(benches);
