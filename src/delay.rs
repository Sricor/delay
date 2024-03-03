use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use tokio::time;
use tracing::{instrument, trace};

use crate::{
    error::DelayError,
    task::{Task, TaskIdentifier},
};

type DelayResult<T> = Result<T, DelayError>;

#[derive(Debug)]
struct DealyTask {
    task: Arc<Task>,
    valid: AtomicBool,
}

impl DealyTask {
    fn with_task(task: Arc<Task>) -> Arc<Self> {
        let item = Self {
            valid: AtomicBool::new(true),
            task,
        };

        Arc::new(item)
    }

    fn disable(&self) {
        self.valid.store(false, Ordering::SeqCst)
    }

    fn is_disable(&self) -> bool {
        !self.valid.load(Ordering::Relaxed)
    }
}

pub struct Delay {
    inventory: Mutex<HashMap<String, Arc<DealyTask>>>,
}

impl Delay {
    pub fn new() -> Self {
        Self {
            inventory: Mutex::new(HashMap::new()),
        }
    }

    pub async fn insert(&self, task: Arc<Task>) -> DelayResult<TaskIdentifier> {
        let mut inventory = self.inventory.lock()?;
        let identifier = task.identifier().clone();

        if inventory.contains_key(&identifier) {
            return Err(DelayError::TaskAlreadyExists(identifier));
        };

        let delay_task = DealyTask::with_task(task);

        inventory.insert(identifier.clone(), delay_task.clone());
        self.executor(delay_task).await;

        Ok(identifier)
    }

    pub async fn remove(&self, identifier: &TaskIdentifier) -> DelayResult<Arc<Task>> {
        let mut inventory = self.inventory.lock()?;
        let delay_task = inventory.remove(identifier);
        if let Some(val) = delay_task {
            val.disable();
            Ok(val.task.clone())
        } else {
            Err(DelayError::TaskNotExists(String::from("value")))
        }
    }

    #[instrument(skip(self))]
    async fn executor(&self, delay_task: Arc<DealyTask>) {
        tokio::spawn(async move {
            'runner: loop {
                if delay_task.is_disable() {
                    trace!("delay task disable");
                    break 'runner;
                };

                delay_task.task.call().await;
            }
        });

        time::sleep(Duration::ZERO).await;
    }
}

#[cfg(test)]
mod tests_delay {
    use crate::task::TaskBuilder;

    use super::*;

    fn simple_delay() -> Delay {
        Delay::new()
    }

    fn simple_task_builder() -> TaskBuilder {
        TaskBuilder::default()
    }

    #[tokio::test]
    async fn test_insert_and_remove() {
        let delay = simple_delay();

        let task = simple_task_builder().build();
        delay.insert(Arc::new(task)).await.unwrap();
        assert_eq!(delay.inventory.lock().unwrap().len(), 1);

        let task = simple_task_builder().build();
        let identifier = task.identifier().clone();
        delay.insert(Arc::new(task)).await.unwrap();
        assert_eq!(delay.inventory.lock().unwrap().len(), 2);

        delay.remove(&identifier).await.unwrap();
        assert_eq!(delay.inventory.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_double_insert() {
        let delay = simple_delay();
        let task = Arc::new(simple_task_builder().build());

        let first = delay.insert(task.clone()).await.unwrap();
        let second = delay.insert(task.clone()).await;

        assert_eq!(&first, task.identifier());
        assert_eq!(second.is_err(), true);
    }

    #[tokio::test]
    async fn test_double_remove() {
        let delay = simple_delay();
        let task = Arc::new(simple_task_builder().build());

        delay.insert(task.clone()).await.unwrap();

        let first = delay.remove(task.identifier()).await.unwrap();
        let second = delay.remove(task.identifier()).await;

        assert_eq!(first.identifier(), task.identifier());
        assert_eq!(second.is_err(), true);
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_insert() {
        let delay = simple_delay();
        let task = Arc::new(simple_task_builder().set_interval_from_secs(1).build());

        delay.insert(task.clone()).await.unwrap();
        assert_eq!(Arc::strong_count(&task), 2);
        assert_eq!(task.running(), 1);
        assert_eq!(task.total_runs(), 0);

        time::sleep(Duration::from_secs(1)).await;
        assert_eq!(Arc::strong_count(&task), 2);
        assert_eq!(task.running(), 1);
        assert_eq!(task.total_runs(), 1);

        time::sleep(Duration::from_secs(3)).await;
        assert_eq!(Arc::strong_count(&task), 2);
        assert_eq!(task.running(), 1);
        assert_eq!(task.total_runs(), 3);
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_stop() {
        let delay = simple_delay();
        let task = Arc::new(simple_task_builder().set_interval_from_secs(1).build());
        let identifier = task.identifier().clone();

        delay.insert(task.clone()).await.unwrap();
        assert_eq!(Arc::strong_count(&task), 2);
        assert_eq!(task.running(), 1);
        assert_eq!(task.total_runs(), 0);

        delay.remove(&identifier).await.unwrap();
        assert_eq!(Arc::strong_count(&task), 2);
        assert_eq!(task.running(), 1);
        assert_eq!(task.total_runs(), 0);

        time::sleep(Duration::from_secs(1)).await;
        assert_eq!(Arc::strong_count(&task), 1);
        assert_eq!(task.running(), 0);
        assert_eq!(task.total_runs(), 1);

        time::sleep(Duration::from_secs(1)).await;
        assert_eq!(Arc::strong_count(&task), 1);
        assert_eq!(task.running(), 0);
        assert_eq!(task.total_runs(), 1);
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_restrat_without_sleep() {
        let delay = simple_delay();
        let task = Arc::new(simple_task_builder().set_interval_from_secs(1).build());
        let identifier = task.identifier().clone();

        delay.insert(task.clone()).await.unwrap();
        assert_eq!(Arc::strong_count(&task), 2);
        assert_eq!(task.running(), 1);
        assert_eq!(task.total_runs(), 0);

        delay.remove(&identifier).await.unwrap();
        assert_eq!(Arc::strong_count(&task), 2);
        assert_eq!(task.running(), 1);
        assert_eq!(task.total_runs(), 0);

        time::sleep(Duration::from_secs(2)).await;
        assert_eq!(Arc::strong_count(&task), 1);
        assert_eq!(task.running(), 0);
        assert_eq!(task.total_runs(), 1);
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_single_concurrent_restart_without_sleep() {
        let delay = simple_delay();
        let task = Arc::new(
            simple_task_builder()
                .set_interval_from_secs(1)
                .set_max_concurrent(1)
                .build(),
        );
        let identifier = task.identifier().clone();

        delay.insert(task.clone()).await.unwrap();
        delay.remove(&identifier).await.unwrap();
        assert_eq!(Arc::strong_count(&task), 2);
        assert_eq!(task.running(), 1);

        delay.insert(task.clone()).await.unwrap();
        delay.remove(&identifier).await.unwrap();
        assert_eq!(Arc::strong_count(&task), 3);
        assert_eq!(task.running(), 1);

        delay.insert(task.clone()).await.unwrap();
        delay.remove(&identifier).await.unwrap();
        assert_eq!(Arc::strong_count(&task), 3);
        assert_eq!(task.running(), 1);

        time::sleep(Duration::from_secs(2)).await;
        assert_eq!(Arc::strong_count(&task), 1);
        assert_eq!(task.running(), 0);
        assert_eq!(task.total_runs(), 1);

        delay.insert(task.clone()).await.unwrap();
        delay.remove(&identifier).await.unwrap();

        time::sleep(Duration::from_secs(2)).await;
        assert_eq!(Arc::strong_count(&task), 1);
        assert_eq!(task.running(), 0);

        assert_eq!(task.total_runs(), 2);
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_multi_concurrent_restart_without_sleep() {
        let delay = simple_delay();
        let task = Arc::new(simple_task_builder().build());
        let identifier = task.identifier().clone();

        delay.insert(task.clone()).await.unwrap();
        delay.remove(&identifier).await.unwrap();
        assert_eq!(Arc::strong_count(&task), 2);
        assert_eq!(task.running(), 1);

        delay.insert(task.clone()).await.unwrap();
        delay.remove(&identifier).await.unwrap();
        assert_eq!(Arc::strong_count(&task), 2);
        assert_eq!(task.running(), 1);

        delay.insert(task.clone()).await.unwrap();
        delay.remove(&identifier).await.unwrap();
        assert_eq!(Arc::strong_count(&task), 2);
        assert_eq!(task.running(), 1);

        time::sleep(Duration::from_secs(1)).await;
        assert_eq!(Arc::strong_count(&task), 1);
        assert_eq!(task.running(), 0);

        assert_eq!(task.total_runs(), 3);
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_multi_concurrent_restart_without_sleep_second() {
        let delay = simple_delay();
        let task = Arc::new(
            simple_task_builder()
            .set_interval_from_secs(2)
            .build());
        let identifier = task.identifier().clone();

        delay.insert(task.clone()).await.unwrap();
        delay.remove(&identifier).await.unwrap();
        assert_eq!(Arc::strong_count(&task), 2);
        assert_eq!(task.running(), 1);

        delay.insert(task.clone()).await.unwrap();
        delay.remove(&identifier).await.unwrap();
        assert_eq!(Arc::strong_count(&task), 3);
        assert_eq!(task.running(), 2);

        delay.insert(task.clone()).await.unwrap();
        delay.remove(&identifier).await.unwrap();
        assert_eq!(Arc::strong_count(&task), 4);
        assert_eq!(task.running(), 3);

        time::sleep(Duration::from_secs(2)).await;
        assert_eq!(Arc::strong_count(&task), 1);
        assert_eq!(task.running(), 0);

        assert_eq!(task.total_runs(), 3);
    }
}
