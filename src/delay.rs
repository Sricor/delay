use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time;

use crate::error::{TaskError, TaskManagerError};

type AsyncTask<T> = Pin<Box<dyn Future<Output = T> + Send + Sync>>;

type TaskId = u64;
type TaskTimeout = Duration;
type TaskInterval = Duration;
type TaskValid = AtomicBool;
type TaskProcess = Box<dyn Fn() -> AsyncTask<()> + Send + Sync>;
type TaskCallback = Box<dyn Fn(Result<(), TaskError>) -> AsyncTask<()> + Send + Sync>;

// ===== Task =====

pub struct Task {
    id: TaskId,
    valid: TaskValid,
    process: TaskProcess,
    interval: TaskInterval,
    callback: Option<TaskCallback>,
    timeout: Option<Duration>,
}

// ===== Task Manager =====
pub struct TaskManager {
    tasks: Mutex<HashMap<TaskId, Arc<Task>>>,
    sender: mpsc::Sender<Arc<Task>>,
}

impl TaskManager {
    // Create a new TaskManager
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel::<Arc<Task>>(32);
        let tasks = Mutex::new(HashMap::new());

        TaskManager::executor(receiver);
        TaskManager { tasks, sender }
    }

    // Add a new task to the TaskManager
    pub fn insert_task(&self, task: Arc<Task>) -> Result<(), TaskManagerError> {
        if let Ok(_) = self.get_task_by_id(task.id) {
            return Err(TaskManagerError::TaskAlreadyExists(task.id));
        }

        let mut tasks = self.tasks.lock()?;
        tasks.insert(task.id, task.clone());

        let sender = self.sender.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(task.interval).await;
                if task.valid.load(Ordering::Relaxed) {
                    sender.send(task.clone()).await.unwrap();
                } else {
                    break;
                }
            }
        });

        Ok(())
    }

    pub fn remove_task(&self, id: TaskId) -> Result<(), TaskManagerError> {
        let _ = self.stop_task(id)?;

        let mut tasks = self.tasks.lock()?;
        match tasks.remove(&id) {
            Some(_) => Ok(()),
            None => Err(TaskManagerError::TaskNotExists(id)),
        }
    }

    pub async fn trigger_task(&self, id: TaskId) -> Result<(), TaskManagerError> {
        let task = self.get_task_by_id(id)?;
        self.sender.send(task).await?;

        Ok(())
    }

    pub fn stop_task(&self, id: TaskId) -> Result<(), TaskManagerError> {
        let task = self.get_task_by_id(id)?;
        if task.valid.load(Ordering::Relaxed) {
            task.valid.store(false, Ordering::Relaxed);
        } else {
            return Err(TaskManagerError::TaskAlreadyStop(id));
        }

        Ok(())
    }

    pub fn start_task(&self, id: TaskId) -> Result<(), TaskManagerError> {
        let task = self.get_task_by_id(id)?;
        if !task.valid.load(Ordering::Relaxed) {
            task.valid.store(true, Ordering::Relaxed);
        } else {
            return Err(TaskManagerError::TaskAlreadyRunning(id));
        }

        Ok(())
    }

    pub fn is_valid_task(&self, id: TaskId) -> Result<bool, TaskManagerError> {
        let task = self.get_task_by_id(id)?;

        Ok(task.valid.load(Ordering::Relaxed))
    }

    pub fn is_exists_task(&self, id: TaskId) -> bool {
        self.get_task_by_id(id).is_ok()
    }

    pub fn get_task_by_id(&self, id: TaskId) -> Result<Arc<Task>, TaskManagerError> {
        let tasks = self.tasks.lock()?;
        let task = tasks.get(&id);
        match task {
            Some(t) => Ok(t.clone()),
            None => Err(TaskManagerError::TaskNotExists(id)),
        }
    }

    pub fn task_count(&self) -> Result<usize, TaskManagerError> {
        let count = self.tasks.lock()?.len();

        Ok(count)
    }

    fn executor(mut receiver: mpsc::Receiver<Arc<Task>>) {
        tokio::spawn(async move {
            while let Some(task) = receiver.recv().await {
                tokio::spawn(async move {
                    let task_result = if let Some(time) = task.timeout.as_ref() {
                        let result = time::timeout(*time, (task.process)()).await;
                        match result {
                            Ok(_) => Ok(()),
                            Err(_) => Err(TaskError::Timeout),
                        }
                    } else {
                        let _ = (task.process)().await;
                        Ok(())
                    };

                    if let Some(callback) = task.callback.as_ref() {
                        callback(task_result).await;
                    }
                });
            }
        });
    }
}

// ===== Task Builder =====
pub struct TaskBuilder {
    id: TaskId,
    valid: TaskValid,
    process: TaskProcess,
    interval: TaskInterval,
    callback: Option<TaskCallback>,
    timeout: Option<Duration>,
}

impl Default for TaskBuilder {
    fn default() -> Self {
        Self {
            id: 0,
            valid: AtomicBool::new(true),
            process: Box::new(move || Box::pin(async {})),
            interval: Duration::from_secs(1),
            callback: None,
            timeout: None,
        }
    }
}

impl TaskBuilder {
    pub fn set_id(mut self, id: TaskId) -> Self {
        self.id = id;
        self
    }

    pub fn set_process<F, Fut>(mut self, process: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + Sync + 'static,
    {
        self.process = Box::new(move || Box::pin(process()));
        self
    }

    pub fn set_callback<F, Fut>(mut self, callback: F) -> Self
    where
        F: Fn(Result<(), TaskError>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + Sync + 'static,
    {
        self.callback = Some(Box::new(move |e| Box::pin(callback(e))));
        self
    }

    pub fn set_valid(mut self, valid: bool) -> Self {
        self.valid = AtomicBool::new(valid);
        self
    }

    pub fn set_timeout(mut self, timeout: TaskTimeout) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn set_timeout_from_secs(mut self, secs: u64) -> Self {
        self.timeout = Some(time::Duration::from_secs(secs));
        self
    }

    pub fn set_interval(mut self, interval: TaskInterval) -> Self {
        self.interval = interval;
        self
    }

    pub fn set_interval_from_secs(mut self, secs: u64) -> Self {
        self.interval = time::Duration::from_secs(secs);
        self
    }

    pub fn build(self) -> Arc<Task> {
        let task = Task {
            id: self.id,
            valid: self.valid,
            interval: self.interval,
            process: self.process,
            callback: self.callback,
            timeout: self.timeout,
        };

        Arc::new(task)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicIsize;

    use super::*;

    fn new_manager() -> TaskManager {
        TaskManager::new()
    }

    #[tokio::test]
    async fn test_trigger_interval_task() {
        let manager = new_manager();

        let run_times = Arc::new(AtomicIsize::new(0));
        let run_times_clone = Arc::clone(&run_times);

        let task = TaskBuilder::default()
            .set_id(1)
            .set_interval(Duration::from_secs(5))
            .set_process(move || {
                let times = Arc::clone(&run_times_clone);
                async move {
                    times.fetch_add(1, Ordering::Relaxed);
                }
            })
            .build();

        manager.insert_task(task).unwrap();

        for _ in 0..5 {
            manager.trigger_task(1).await.unwrap();
        }

        tokio::time::sleep(Duration::from_secs(4)).await;
        assert_eq!(run_times.load(Ordering::Relaxed), 5);
    }

    #[tokio::test]
    async fn test_stop_interval_task() {
        let manager = new_manager();

        let run_times = Arc::new(AtomicIsize::new(0));
        let run_times_clone = Arc::clone(&run_times);

        let task = TaskBuilder::default()
            .set_id(1)
            .set_interval(Duration::from_secs(2))
            .set_process(move || {
                let times = Arc::clone(&run_times_clone);
                async move {
                    times.fetch_add(1, Ordering::Relaxed);
                }
            })
            .build();

        manager.insert_task(task).unwrap();
        time::sleep(Duration::from_secs(3)).await;
        assert_eq!(run_times.load(Ordering::Relaxed), 1);
        manager.stop_task(1).unwrap();

        time::sleep(Duration::from_secs(3)).await;
        assert_eq!(run_times.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_repeat_stop_interval_task() {
        let manager = new_manager();

        let run_times = Arc::new(AtomicIsize::new(0));
        let run_times_clone = Arc::clone(&run_times);

        let task = TaskBuilder::default()
            .set_id(1)
            .set_interval(Duration::from_secs(2))
            .set_process(move || {
                let times = Arc::clone(&run_times_clone);
                async move {
                    times.fetch_add(1, Ordering::Relaxed);
                }
            })
            .build();
        manager.insert_task(task).unwrap();
        assert_eq!(manager.stop_task(1), Ok(()));
        assert_eq!(
            manager.stop_task(1),
            Err(TaskManagerError::TaskAlreadyStop(1))
        );
    }

    #[tokio::test]
    async fn test_repeat_start_interval_task() {
        let manager = new_manager();

        let run_times = Arc::new(AtomicIsize::new(0));
        let run_times_clone = Arc::clone(&run_times);

        let task = TaskBuilder::default()
            .set_id(1)
            .set_interval(Duration::from_secs(2))
            .set_process(move || {
                let times = Arc::clone(&run_times_clone);
                async move {
                    times.fetch_add(1, Ordering::Relaxed);
                }
            })
            .build();
        manager.insert_task(task).unwrap();
        assert_eq!(
            manager.start_task(1),
            Err(TaskManagerError::TaskAlreadyRunning(1))
        );
    }

    #[tokio::test]
    async fn test_remove_task() {
        let manager = new_manager();
        let task = TaskBuilder::default().set_id(1).build();

        manager.insert_task(task).unwrap();
        assert_eq!(manager.task_count().unwrap(), 1);

        manager.remove_task(1).unwrap();
        assert_eq!(manager.task_count().unwrap(), 0);

        // repeat remove
        assert_eq!(
            manager.remove_task(1),
            Err(TaskManagerError::TaskNotExists(1))
        );
    }

    #[tokio::test]
    async fn test_task_timeout() {
        let manager = new_manager();
        let task = TaskBuilder::default()
            .set_timeout_from_secs(1)
            .set_process(|| async {
                time::sleep(time::Duration::from_secs(2)).await;
            })
            .set_callback(|result| async move { assert_eq!(result, Err(TaskError::Timeout)) })
            .build();

        manager.insert_task(task).unwrap();
        time::sleep(Duration::from_secs(3)).await;
    }
}
