# Custom exception: ExtractionError
class ExtractionError(Exception):
    """Raised when data extraction fails."""

    def __init__(self, message: str):
        self.message = message # New attribute
        super().__init__(self.message) # Add new attribute to the inheritance

# Custom exception: TransformationError
class TransformationError(Exception):
    """Raised when data transformation fails."""

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)

# Custom exception: LoadError
class LoadError(Exception):
    """Raised when data loading fails."""

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)