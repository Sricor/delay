use std::error::Error;
use std::fmt;
use std::sync::PoisonError;

use tokio::sync::mpsc::error::SendError;

use task::TaskIdentifier;

use crate::task;

// ===== TaskManager Error =====
#[derive(Debug, PartialEq, Clone)]
pub enum DelayError {
    TaskNotExists(TaskIdentifier),
    TaskAlreadyExists(TaskIdentifier),
    TaskAlreadyRemove(TaskIdentifier),
    TaskAlreadyRunning(TaskIdentifier),
    TaskAlreadyStop(TaskIdentifier),
    TaskChannelSendError,
    TasksLockingError,
}

impl Error for DelayError {}

impl fmt::Display for DelayError {
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

impl<T> From<SendError<T>> for DelayError {
    fn from(_: SendError<T>) -> Self {
        DelayError::TaskChannelSendError
    }
}

impl<T> From<PoisonError<T>> for DelayError {
    fn from(_: PoisonError<T>) -> Self {
        DelayError::TasksLockingError
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
