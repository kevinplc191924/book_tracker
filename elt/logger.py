import logging

def get_logger(name: str) -> logging.Logger:
    """Set a logger instance."""
    
    # Retrieve a logger instance with the given name
    logger = logging.getLogger(name)
    
    # Prevent multiple handlers from being added to the same logger
    # Handler: decides where the logs go (console, file, etc.)
    if not logger.hasHandlers():
        # Create a handler to output logs to the console
        handler = logging.StreamHandler()
        # How the message will look like
        # Complete format: "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
        formatter = logging.Formatter(
            '%(message)s'
        )
        # Set the format to the handler
        handler.setFormatter(formatter)
        # Add the handler to the logger
        logger.addHandler(handler)
        # Set the minimum severity level for messages to be logged
        logger.setLevel(logging.INFO)
    
    return logger