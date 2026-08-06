//! Main catalog worker library shared by the legacy ingest entry point and
//! the consolidated four-container worker entry point.

pub mod jobs;
pub mod runtime;
mod scheduler;
