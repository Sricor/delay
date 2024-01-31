use std::error::Error;
use std::fmt;
use std::sync::PoisonError;

use tokio::sync::mpsc::error::SendError;

// ===== TaskManager Error =====
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum TaskManagerError {
    TaskNotExists(u64),
    TaskAlreadyExists(u64),
    TaskAlreadyRemove(u64),
    TaskAlreadyRunning(u64),
    TaskAlreadyStop(u64),
    TaskChannelSendError,
    TasksLockingError,
}

impl Error for TaskManagerError {}

impl fmt::Display for TaskManagerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let message = match self {
            Self::TaskNotExists(id) => format!("task {id} does not exist"),
            Self::TaskAlreadyExists(id) => format!("task {id} already exists"),
            Self::TaskAlreadyRemove(id) => format!("task {id} has been removed"),
            Self::TaskAlreadyRunning(id) => format!("task {id} is already running"),
            Self::TaskAlreadyStop(id) => format!("task {id} has been stopped"),
            Self::TaskChannelSendError => format!("task send to channel error"),
            Self::TasksLockingError => format!("locking tasks error"),
        };
        write!(f, "{}", message)
    }
}

impl<T> From<SendError<T>> for TaskManagerError {
    fn from(_: SendError<T>) -> Self {
        TaskManagerError::TaskChannelSendError
    }
}

impl<T> From<PoisonError<T>> for TaskManagerError {
    fn from(_: PoisonError<T>) -> Self {
        TaskManagerError::TasksLockingError
    }
}

// ===== Task Error =====

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum TaskError {
    Timeout,
}

impl Error for TaskError {}

impl fmt::Display for TaskError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", Into::<&str>::into(*self))
    }
}

impl Into<&str> for TaskError {
    fn into(self) -> &'static str {
        match self {
            Self::Timeout => "running timeout",
        }
    }
}
