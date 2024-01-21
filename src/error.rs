use std::error::Error;
use std::fmt;
use std::sync::PoisonError;

use tokio::sync::mpsc::error::SendError;

// ===== TaskManager Error =====
#[derive(Debug, Clone, Copy)]
pub enum TaskManagerError {
    TaskNotExists,
    TaskAlreadyExists,
    TaskAlreadyRemove,
    TaskChannelSendError,
    TasksLockingError,
}

impl Error for TaskManagerError {}

impl fmt::Display for TaskManagerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", Into::<&str>::into(*self))
    }
}

impl Into<&str> for TaskManagerError {
    fn into(self) -> &'static str {
        match self {
            Self::TaskNotExists => "task does not exist",
            Self::TaskAlreadyExists => "task already exists",
            Self::TaskAlreadyRemove => "task has been removed",
            Self::TaskChannelSendError => "task channel send error",
            Self::TasksLockingError => "locking tasks error",
        }
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
