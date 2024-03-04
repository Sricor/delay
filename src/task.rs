use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::time;
use tracing::{instrument, trace};
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

    pub fn running(&self) -> usize {
        self.running.load(Ordering::Acquire)
    }

    pub fn total_runs(&self) -> usize {
        self.total_runs.load(Ordering::Acquire)
    }

    pub async fn trigger(&self) {
        trace!("trigger");
        (self.process)().await;
    }

    pub async fn run(self: Arc<Self>) {
        tokio::spawn(async move { self.call().await });

        time::sleep(Duration::ZERO).await;
    }

    #[instrument]
    pub async fn call(&self) {
        if self.is_reached_max_concurrent() {
            trace!("reached max concurrent");
            time::sleep(Duration::ZERO).await;
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

    pub(crate) async fn sleep(&self) {
        trace!("sleep");
        time::sleep(self.interval).await;
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
            return self.running() >= value;
        }

        false
    }
}

impl Debug for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Task")
            .field("identifier", &self.identifier)
            .field("running", &self.running)
            .field("total_runs", &self.total_runs)
            .finish()
    }
}

// ===== Task Builder =====
pub struct TaskBuilder {
    pub identifier: TaskIdentifier,
    pub interval: Duration,
    pub process: Box<dyn Fn() -> PinBoxFuture<()> + Send + Sync>,
    pub timeout: Option<Duration>,
    pub max_concurrent: Option<usize>,
}

impl Default for TaskBuilder {
    fn default() -> Self {
        Self {
            identifier: Uuid::new_v4().to_string(),
            process: Box::new(move || Box::pin(async {})),
            interval: Duration::default(),
            timeout: None,
            max_concurrent: None,
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

    pub fn set_max_concurrent(mut self, count: usize) -> Self {
        self.max_concurrent = Some(count);
        self
    }

    pub fn build(self) -> Task {
        Task {
            identifier: self.identifier,
            running: AtomicUsize::new(0),
            total_runs: AtomicUsize::new(0),
            max_concurrent: self.max_concurrent,
            interval: self.interval,
            process: self.process,
            timeout: self.timeout,
        }
    }
}

#[cfg(test)]
mod tests_task {
    use super::*;

    impl Task {
        fn strong_count(self: &Arc<Self>) -> usize {
            Arc::strong_count(self)
        }
    }

    #[tokio::test]
    async fn test_task_runing() {
        let task = TaskBuilder::default().set_interval_from_secs(1).build();
        let task = Arc::new(task);

        let number = 10;

        for _ in 0..number {
            let task = task.clone();
            task.run().await;
        }

        assert_eq!(task.running(), 10);
        assert_eq!(task.total_runs(), 0);
        assert_eq!(task.strong_count(), 11);

        time::sleep(Duration::from_secs(2)).await;
        assert_eq!(task.running(), 0);
        assert_eq!(task.total_runs(), 10);
        assert_eq!(task.strong_count(), 1);
    }

    #[tokio::test]
    async fn test_task_runing_multi() {
        let task = TaskBuilder::default().set_interval_from_secs(1).build();
        let task = Arc::new(task);

        let task_sec = TaskBuilder::default().set_interval_from_secs(2).build();
        let task_sec = Arc::new(task_sec);

        let number = 10;

        for _ in 0..number {
            let task = task.clone();
            task.run().await;

            let task_sec = task_sec.clone();
            task_sec.run().await;
        }

        assert_eq!(task.running(), 10);
        assert_eq!(task.total_runs(), 0);
        assert_eq!(task.strong_count(), 11);

        assert_eq!(task_sec.running(), 10);
        assert_eq!(task_sec.total_runs(), 0);
        assert_eq!(task_sec.strong_count(), 11);

        time::sleep(Duration::from_secs(1)).await;
        assert_eq!(task.running(), 0);
        assert_eq!(task.total_runs(), 10);
        assert_eq!(task.strong_count(), 1);

        assert_eq!(task_sec.running(), 10);
        assert_eq!(task_sec.total_runs(), 0);
        assert_eq!(task_sec.strong_count(), 11);

        time::sleep(Duration::from_secs(1)).await;
        assert_eq!(task_sec.running(), 0);
        assert_eq!(task_sec.total_runs(), 10);
        assert_eq!(task_sec.strong_count(), 1);
    }

    #[tokio::test]
    async fn test_task_runing_limit_concurrent() {
        let task = TaskBuilder::default()
            .set_interval_from_secs(1)
            .set_max_concurrent(5)
            .build();
        let task = Arc::new(task);

        let number = 15;

        for _ in 0..number {
            let task = task.clone();
            task.run().await;
        }

        assert_eq!(task.running(), 5);
        assert_eq!(task.total_runs(), 0);
        assert_eq!(task.is_reached_max_concurrent(), true);

        time::sleep(Duration::from_secs(2)).await;
        assert_eq!(task.running(), 0);
        assert_eq!(task.total_runs(), 5);
        assert_eq!(task.strong_count(), 1);
        assert_eq!(task.is_reached_max_concurrent(), false);
    }

    #[tokio::test]
    async fn test_task_runing_zero_concurrent() {
        let task = TaskBuilder::default().set_max_concurrent(0).build();
        let task = Arc::new(task);

        let number = 10;

        for _ in 0..number {
            let task = task.clone();
            task.run().await;
        }

        assert_eq!(task.running(), 0);
        assert_eq!(task.total_runs(), 0);
        assert_eq!(task.is_reached_max_concurrent(), true);

        time::sleep(Duration::from_secs(1)).await;
        assert_eq!(task.running(), 0);
        assert_eq!(task.total_runs(), 0);
        assert_eq!(task.strong_count(), 1);
        assert_eq!(task.is_reached_max_concurrent(), true);
    }

    #[tokio::test]
    async fn test_task_runing_one_concurrent() {
        let task = TaskBuilder::default().set_max_concurrent(1).build();
        let task = Arc::new(task);

        let number = 1;

        for _ in 0..number {
            let task = task.clone();
            task.run().await;
        }

        assert_eq!(task.running(), 1);
        assert_eq!(task.total_runs(), 0);
        assert_eq!(task.strong_count(), 2);
        assert_eq!(task.is_reached_max_concurrent(), true);

        time::sleep(Duration::from_secs(1)).await;
        assert_eq!(task.running(), 0);
        assert_eq!(task.total_runs(), 1);
        assert_eq!(task.strong_count(), 1);
        assert_eq!(task.is_reached_max_concurrent(), false);
    }
}
