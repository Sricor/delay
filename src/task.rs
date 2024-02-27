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
    
    pub(crate) async fn is_reached_max_runs_count (self: &Arc<Self>) -> bool {
        if let Some(v) = self.max_run_count {
            return v >
        }
    }
}

// // ===== Task Builder =====
// pub struct TaskBuilder {
//     id: TaskIdentifier,
//     valid: TaskValid,
//     process: TaskProcess,
//     interval: TaskInterval,
//     callback: Option<TaskCallback>,
//     max_run_count: Option<TaskRunCount>,
//     timeout: Option<Duration>,
// }

// impl Default for TaskBuilder {
//     fn default() -> Self {
//         Self {
//             id: Uuid::new_v4().to_string(),
//             valid: AtomicBool::new(true),
//             process: Box::new(move || Box::pin(async {})),
//             interval: Duration::from_secs(5),
//             max_run_count: None,
//             callback: None,
//             timeout: None,
//         }
//     }
// }

// impl TaskBuilder {
//     /// The unique identifier of the task, through which the deletion task can be started or stopped.
//     pub fn set_id(mut self, id: TaskIdentifier) -> Self {
//         self.id = id;
//         self
//     }

//     /// Task execution code block
//     pub fn set_process<F, Fut>(mut self, process: F) -> Self
//     where
//         F: Fn() -> Fut + Send + Sync + 'static,
//         Fut: Future<Output = ()> + Send + Sync + 'static,
//     {
//         self.process = Box::new(move || Box::pin(process()));
//         self
//     }

//     /// Callback process after task execution is completed
//     pub fn set_callback<F, Fut>(mut self, callback: F) -> Self
//     where
//         F: Fn(Result<(), TaskError>) -> Fut + Send + Sync + 'static,
//         Fut: Future<Output = ()> + Send + Sync + 'static,
//     {
//         self.callback = Some(Box::new(move |e| Box::pin(callback(e))));
//         self
//     }

//     /// The validity of the task. When it is false, the task created will not be automatically executed.
//     pub fn set_valid(mut self, valid: bool) -> Self {
//         self.valid = AtomicBool::new(valid);
//         self
//     }

//     /// The timeout period of the task. When the task execution times out, the timeout error will be passed to the callback.
//     pub fn set_timeout(mut self, timeout: TaskTimeout) -> Self {
//         self.timeout = Some(timeout);
//         self
//     }

//     pub fn set_timeout_from_secs(mut self, secs: u64) -> Self {
//         self.timeout = Some(time::Duration::from_secs(secs));
//         self
//     }

//     /// Task execution cycle interval
//     pub fn set_interval(mut self, interval: TaskInterval) -> Self {
//         self.interval = interval;
//         self
//     }

//     pub fn set_interval_from_secs(mut self, secs: u64) -> Self {
//         self.interval = time::Duration::from_secs(secs);
//         self
//     }

//     pub fn set_max_run_count(mut self, count: u32) -> Self {
//         self.max_run_count = Some(AtomicU32::new(count));
//         self
//     }

//     pub fn build(self) -> Arc<Task> {
//         let task = Task {
//             id: self.id,
//             valid: self.valid,
//             running: AtomicBool::new(false),
//             run_count: AtomicU32::new(0),
//             max_run_count: self.max_run_count,
//             interval: self.interval,
//             process: self.process,
//             callback: self.callback,
//             timeout: self.timeout,
//         };

//         Arc::new(task)
//     }
// }
