"""
Custom exceptions for the application
"""


class FileProcessingException(Exception):
    """Exception raised when file processing fails"""
    
    def __init__(self, message: str, details: dict = None):
        self.message = message
        self.details = details or {}
        super().__init__(self.message)


class DatabaseException(Exception):
    """Exception raised when database operations fail"""
    
    def __init__(self, message: str, details: dict = None):
        self.message = message
        self.details = details or {}
        super().__init__(self.message)


class NotFoundException(Exception):
    """Exception raised when a resource is not found"""
    
    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)

