//! Background job primitives for the TMDB mirror.

mod error;
mod model;
mod repository;
mod worker;

pub use error::{JobError, ValidationError};
pub use model::{
    ClaimedJob, FailureDisposition, Job, JobId, JobStatus, NewJob, SubmitOutcome, WorkerId,
};
pub use repository::JobRepository;
pub use worker::{
    JobExecutionError, JobExecutor, Worker, WorkerConfig, WorkerConfigError, WorkerError,
};
