// ===== Task Manager =====
pub struct TaskManager {
    tasks: Mutex<HashMap<TaskIdentifier, Arc<Task>>>,
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
    pub fn insert_task(&self, task: Arc<Task>) -> Result<TaskIdentifier, TaskManagerError> {
        let id = task.id.clone();
        if self.is_exists_task(&task.id) {
            return Err(TaskManagerError::TaskAlreadyExists(task.id.clone()));
        }

        {
            let mut tasks = self.tasks.lock()?;
            tasks.insert(task.id.clone(), task.clone());
        };

        if task.valid.load(Ordering::Relaxed) {
            self.run_task(task);
        }

        Ok(id)
    }

    pub fn remove_task(&self, id: &TaskIdentifier) -> Result<(), TaskManagerError> {
        let _ = self.stop_task(id)?;

        let mut tasks = self.tasks.lock()?;
        match tasks.remove(id) {
            Some(_) => Ok(()),
            None => Err(TaskManagerError::TaskNotExists(id.clone())),
        }
    }

    /// Forcibly call a task process, regardless of task validity
    pub async fn trigger_task(&self, id: &TaskIdentifier) -> Result<(), TaskManagerError> {
        let task = self.get_task_by_id(id)?;
        self.sender.send(task).await?;

        Ok(())
    }

    pub fn stop_task(&self, id: &TaskIdentifier) -> Result<(), TaskManagerError> {
        let task = self.get_task_by_id(id)?;
        if task.valid.load(Ordering::Relaxed) {
            task.valid.store(false, Ordering::Relaxed);
        } else {
            return Err(TaskManagerError::TaskAlreadyStop(id.clone()));
        }

        Ok(())
    }

    pub fn start_task(&self, id: &TaskIdentifier) -> Result<(), TaskManagerError> {
        let task = self.get_task_by_id(id)?;
        if !task.valid.load(Ordering::Relaxed) {
            task.valid.store(true, Ordering::Relaxed);
            self.run_task(task);
        } else {
            return Err(TaskManagerError::TaskAlreadyRunning(id.clone()));
        }

        Ok(())
    }

    pub fn is_valid_task(&self, id: &TaskIdentifier) -> Result<bool, TaskManagerError> {
        let task = self.get_task_by_id(id)?;

        Ok(task.valid.load(Ordering::Relaxed))
    }

    pub fn is_exists_task(&self, id: &TaskIdentifier) -> bool {
        self.get_task_by_id(id).is_ok()
    }

    pub fn get_task_by_id(&self, id: &TaskIdentifier) -> Result<Arc<Task>, TaskManagerError> {
        let tasks = self.tasks.lock()?;
        let task = tasks.get(id);
        match task {
            Some(t) => Ok(t.clone()),
            None => Err(TaskManagerError::TaskNotExists(id.clone())),
        }
    }

    pub fn task_count(&self) -> Result<usize, TaskManagerError> {
        let count = self.tasks.lock()?.len();

        Ok(count)
    }

    fn run_task(&self, task: Arc<Task>) {
        let sender = self.sender.clone();
        tokio::spawn(async move {
            if task.running.load(Ordering::Relaxed) {
                return;
            }

            task.running.store(true, Ordering::Relaxed);
            'runner: loop {
                tokio::time::sleep(task.interval).await;

                // TODO: Max running

                if task.valid.load(Ordering::Relaxed) {
                    sender.send(task.clone()).await.unwrap();
                } else {
                    break 'runner;
                }
            }

            task.running.store(false, Ordering::Relaxed);
        });
    }

    fn executor(mut receiver: mpsc::Receiver<Arc<Task>>) {
        tokio::spawn(async move {
            while let Some(task) = receiver.recv().await {
                tokio::spawn(async move {
                    // Determine whether the maximum number of runs has been reached
                    if let Some(limit) = &task.max_run_count {
                        let max_run_count = limit.load(Ordering::Relaxed);
                        if task.run_count.load(Ordering::Relaxed) >= max_run_count {
                            task.valid.store(false, Ordering::Relaxed);
                            return;
                        }
                    }

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

                    // add one to the number of run count
                    task.run_count.fetch_add(1, Ordering::Relaxed);
                });
            }
        });
    }
}