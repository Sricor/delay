use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::time;
use uuid::Uuid;

pub type TaskIdentifier = String;
pub type PinBoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + Sync>>;

// ===== Task =====
pub struct Task {
    /// Task identifier
    identifier: TaskIdentifier,

    /// Running interval
    interval: Duration,

    /// Number of running
    running: AtomicUsize,

    /// Number of runs completed
    total_runs: AtomicUsize,

    /// Actual execution of the task closure
    process: Box<dyn Fn() -> PinBoxFuture<()> + Send + Sync>,

    /// Execution timeout
    timeout: Option<Duration>,

    /// The maximum number of runs, when reached, the task validity is false
    max_concurrent: Option<usize>,
}

impl Task {
    pub fn identifier(&self) -> &TaskIdentifier {
        &self.identifier
    }

    pub async fn trigger(&self) {
        (self.process)().await;
    }

    pub fn running(&self) -> usize {
        self.running.load(Ordering::Acquire)
    }

    pub fn total_runs(&self) -> usize {
        self.total_runs.load(Ordering::Acquire)
    }

    pub(crate) async fn sleep(&self) {
        time::sleep(self.interval).await
    }

    pub(crate) fn total_runs_fetch_add(&self, val: usize) {
        self.total_runs.fetch_add(val, Ordering::Relaxed);
    }

    pub(crate) fn running_fetch_add(&self, val: usize) {
        self.running.fetch_add(val, Ordering::Release);
    }

    pub(crate) fn running_fetch_sub(&self, val: usize) {
        self.running.fetch_sub(val, Ordering::Release);
    }

    pub(crate) fn is_reached_max_concurrent(&self) -> bool {
        if let Some(value) = self.max_concurrent {
            return self.running() > value;
        }

        false
    }

    pub async fn run(&self) {
        if self.is_reached_max_concurrent() {
            return;
        }

        self.running_fetch_add(1);
        self.sleep().await;

        if let Some(duration) = &self.timeout {
            let _ = time::timeout(duration.clone(), self.trigger()).await;
        } else {
            let _ = self.trigger().await;
        };

        self.total_runs_fetch_add(1);
        self.running_fetch_sub(1);
    }
}

// ===== Task Builder =====
pub struct TaskBuilder {
    pub identifier: TaskIdentifier,
    pub interval: Duration,
    pub process: Box<dyn Fn() -> PinBoxFuture<()> + Send + Sync>,
    pub timeout: Option<Duration>,
    pub max_run_count: Option<usize>,
}

impl Default for TaskBuilder {
    fn default() -> Self {
        Self {
            identifier: Uuid::new_v4().to_string(),
            process: Box::new(move || Box::pin(async {})),
            interval: Duration::default(),
            timeout: None,
            max_run_count: None,
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
        F: Fn() -> PinBoxFuture<()> + Send + Sync + 'static,
    {
        self.process = Box::new(process);
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

    pub fn build(self) -> Task {
        Task {
            identifier: self.identifier,
            running: AtomicUsize::new(0),
            total_runs: AtomicUsize::new(0),
            max_concurrent: self.max_run_count,
            interval: self.interval,
            process: self.process,
            timeout: self.timeout,
        }
    }
}

#[cfg(test)]
mod tests_task {
    use super::*;

    fn simple_task() -> Task {
        TaskBuilder::default().build()
    }

    #[tokio::test]
    async fn test_task_runing() {
        let task = simple_task();
        task.run().await;
        task.run().await;
        assert_eq!(task.total_runs(), 1);
    }
}
