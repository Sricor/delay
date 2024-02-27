use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::time;
use uuid::Uuid;

pub type TaskIdentifier = String;
pub type PinBoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + Sync>>;

// ===== Task =====
pub struct Task {
    /// Task identifier
    identifier: TaskIdentifier,

    /// Task valid
    valid: AtomicBool,

    /// Running interval
    interval: Duration,

    /// Number of running
    running: AtomicU32,

    /// Number of runs completed
    run_count: AtomicU32,

    /// Actual execution of the task closure
    process: Box<dyn Fn() -> PinBoxFuture<()> + Send + Sync>,

    /// Execution timeout
    timeout: Option<Duration>,

    /// The maximum number of runs, when reached, the task validity is false
    max_run_count: Option<usize>,

    /// Runs after task execution is complete
    callback: Option<Box<dyn Fn() -> PinBoxFuture<()> + Send + Sync>>,
}

impl Task {
    pub fn identifier(&self) -> &TaskIdentifier {
        &self.identifier
    }

    pub async fn trigger(&self) {
        (self.process)().await;
    }

    pub(crate) fn strong_count(self: &Arc<Self>) -> usize {
        Arc::strong_count(self)
    }

    pub(crate) async fn sleep(&self) {
        time::sleep(self.interval).await
    }
}



// ===== Task Builder =====
pub struct TaskBuilder {
    identifier: TaskIdentifier,
    interval: Duration,
    process: Box<dyn Fn() -> PinBoxFuture<()> + Send + Sync>,
    timeout: Option<Duration>,
    max_run_count: Option<usize>,
    callback: Option<Box<dyn Fn() -> PinBoxFuture<()> + Send + Sync>>,
}

impl Default for TaskBuilder {
    fn default() -> Self {
        Self {
            identifier: Uuid::new_v4().to_string(),
            process: Box::new(move || Box::pin(async {})),
            ..Default::default()
        }
    }
}

impl TaskBuilder {
    /// The unique identifier of the task, through which the deletion task can be started or stopped.
    pub fn set_identifier(mut self, identifier: TaskIdentifier) -> Self {
        self.identifier = identifier;
        self
    }

    /// Task execution code block
    pub fn set_process<F>(mut self, process: F) -> Self
    where
        F: Fn() -> PinBoxFuture<()> + Send + Sync + 'static
    {
        self.process = Box::new(process);
        self
    }

    /// Callback process after task execution is completed
    pub fn set_callback<F>(mut self, callback: F) -> Self
    where
        F: Fn() -> PinBoxFuture<()> + Send + Sync + 'static
    {
        self.callback = Some(Box::new(callback));
        self
    }

    /// The timeout period of the task. When the task execution times out, the timeout error will be passed to the callback.
    pub fn set_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn set_timeout_from_secs(mut self, secs: u64) -> Self {
        self.timeout = Some(time::Duration::from_secs(secs));
        self
    }

    /// Task execution cycle interval
    pub fn set_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    pub fn set_interval_from_secs(mut self, secs: u64) -> Self {
        self.interval = time::Duration::from_secs(secs);
        self
    }

    pub fn set_max_run_count(mut self, count: usize) -> Self {
        self.max_run_count = Some(count);
        self
    }

    pub fn build(self) -> Arc<Task> {
        let task = Task {
            identifier: self.identifier,
            valid: AtomicBool::new(true),
            running: AtomicU32::new(0),
            run_count: AtomicU32::new(0),
            max_run_count: self.max_run_count,
            interval: self.interval,
            process: self.process,
            callback: self.callback,
            timeout: self.timeout,
        };

        Arc::new(task)
    }
}
