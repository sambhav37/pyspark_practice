import logging
import sys

def get_logger(name):
    """Returns a standardized logger instance."""
    logger = logging.getLogger(name)
    
    # Only add handlers if they don't exist to prevent duplicate logs
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        # Format: [Timestamp] [Level] [File Name]: Message
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(name)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # Stream to console (stdout)
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
    return logger