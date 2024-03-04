# Delay
Asynchronous delayed tasks using tokio runtime.

## Examples

```rust
use delay::prelude::{time, TaskBuilder};

#[tokio::main]
async fn main() {
    // Create async task
    // Will print every 3 seconds
    let task = TaskBuilder::default()
        .set_interval(time::Duration::from_secs(3))
        .set_process(|| async {
            println!("Run an asynchronous task");
        })
        .build();

    task.run().await;

    // Wait for async task to complete
    time::sleep(time::Duration::from_secs(10)).await;
}

```


```rust
use delay::prelude::{time, TaskBuilder, TaskManager};

#[tokio::main]
async fn main() {
    // This task will time out
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

    task.run().await;

    // Wait for async task to complete
    time::sleep(time::Duration::from_secs(3)).await;
}

```