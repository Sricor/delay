use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::{
    error::DelayError,
    task::{Task, TaskIdentifier},
};

type DelayResult<T> = Result<T, DelayError>;

pub struct Delay {
    sender: Sender<Arc<Task>>,
    inventory: Mutex<HashMap<String, Arc<Task>>>,
}

impl Delay {
    // Create a new TaskManager
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(32);
        let inventory = Mutex::new(HashMap::new());

        // Self::guard(receiver);
        Self { inventory, sender }
    }

    pub fn insert(&self, task: Task) -> DelayResult<()> {
        let identifier = task.identifier().clone();

        if let Some(_) = self.get(task.identifier()) {
            return Err(DelayError::TaskAlreadyExists(identifier));
        };

        let mut inventory = self.inventory.lock()?;
        inventory.insert(identifier.clone(), Arc::new(task));

        Ok(())
    }

    pub fn remove(&self, identifier: &TaskIdentifier) -> DelayResult<Task> {
        if let None = self.get(identifier) {
            return Err(DelayError::TaskNotExists(identifier.clone()));
        };

        let mut inventory = self.inventory.lock().unwrap();
        let task = inventory.remove(identifier).unwrap();
        let result = Arc::into_inner(task).unwrap();

        Ok(result)
    }

    fn get(&self, identifier: &TaskIdentifier) -> Option<Arc<Task>> {
        let inventory = self.inventory.lock().unwrap();
        let task = inventory.get(identifier)?;

        Some(task.clone())
    }

    fn executor(task: Arc<Task>) {
        tokio::spawn(async move {
            task.sleep().await;
            task.trigger().await;
        });
    }

    // fn guard(mut receiver: Receiver<Arc<Task>>) {
    //     tokio::spawn(async move {
    //         while let Some(task) = receiver.recv().await {
    //             if task.strong_count() > 2 {
    //                 continue;
    //             }

    //             Self::executor(task);
    //         }
    //     });
    // }
}

#[cfg(test)]
mod tests_delay {
    use super::*;

    fn simple_delay() -> Delay {
        Delay::new()
    }

    // fn simple_task() -> Task {
    //     Task::default()
    // }
    // #[test]
    // fn test_insert() {
    //     let delay = simple_delay();
    //     delay.insert().unwrap()
    // }
}
