"""Custom pipeline exceptions."""


class PipelineError(Exception):
    """Base class for all pipeline errors."""


class SchemaError(PipelineError):
    """Raised when a required column is missing (subtractive drift)."""


class IngestionError(PipelineError):
    """Raised when a source file cannot be read."""


class StateError(PipelineError):
    """Raised when a state file (e.g. watermarks.json) is corrupt or unreadable."""
