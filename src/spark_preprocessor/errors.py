"""Custom error types for spark-preprocessor."""


class SparkPreprocessorError(Exception):
    """Base error for spark-preprocessor."""


class ConfigurationError(SparkPreprocessorError):
    """Raised when configuration is invalid or incomplete."""


class ValidationError(SparkPreprocessorError):
    """Raised when validation fails."""


class FeatureNotFoundError(SparkPreprocessorError):
    """Raised when a feature key cannot be resolved."""


class CompileError(SparkPreprocessorError):
    """Raised when compilation fails."""
