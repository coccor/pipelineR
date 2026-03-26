use thiserror::Error;

/// Error during transform step execution (batch-level).
#[derive(Debug, Error)]
pub enum TransformError {
    /// An expression evaluation failed on a specific record.
    #[error("eval error on record {index}: {source}")]
    EvalFailed {
        /// Index of the record that caused the error.
        index: usize,
        /// The underlying evaluation error.
        source: EvalError,
    },

    /// A named step failed with a message.
    #[error("step '{step}' failed: {message}")]
    StepFailed {
        /// Name of the step that failed.
        step: String,
        /// Human-readable error message.
        message: String,
    },
}

/// Error during expression evaluation (record-level).
#[derive(Debug, Error, Clone)]
pub enum EvalError {
    /// A referenced field was not found in the record.
    #[error("field not found: {0}")]
    FieldNotFound(String),

    /// A value had an unexpected type.
    #[error("type mismatch: expected {expected}, got {got}")]
    TypeMismatch {
        /// The expected type name.
        expected: String,
        /// The actual type name.
        got: String,
    },

    /// An array index was out of bounds.
    #[error("index {index} out of bounds for array of length {length}")]
    IndexOutOfBounds {
        /// The index that was accessed.
        index: usize,
        /// The length of the array.
        length: usize,
    },

    /// A null value appeared where a non-null value was required.
    #[error("null value where non-null required")]
    UnexpectedNull,

    /// An unknown function was called.
    #[error("unknown function: {0}")]
    UnknownFunction(String),

    /// A function was called with the wrong number of arguments.
    #[error("wrong argument count for {function}: expected {expected}, got {got}")]
    ArityMismatch {
        /// The function name.
        function: String,
        /// The expected argument count.
        expected: usize,
        /// The actual argument count.
        got: usize,
    },

    /// A parse error occurred.
    #[error("parse error: {0}")]
    ParseError(String),

    /// A custom error message.
    #[error("{0}")]
    Custom(String),
}
