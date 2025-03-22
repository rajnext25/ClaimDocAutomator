# from pathlib import Path
# from configparser import ConfigParser
# from file_processor import (
#     ConfigurationManager, 
#     DatabaseManager, 
#     OCRProcessor, 
#     ClaimProcessor
# )
# from watchdog.observers import Observer
# from watchdog.events import FileSystemEventHandler
# import time
# import os

# WATCH_DIRECTORY = "D:\\psl\\ClaimProcessing\\incoming_files"

# class DocumentHandler(FileSystemEventHandler):
#     def __init__(self):
#         # Load configuration and initialize components
#         self.db_config, self.dir_config = ConfigurationManager.load_config()
#         self.db_manager = DatabaseManager(self.db_config)
#         self.ocr_processor = OCRProcessor(self.dir_config.tesseract_cmd)
#         self.claim_processor = ClaimProcessor(
#             self.db_manager, 
#             self.ocr_processor, 
#             self.dir_config
#         )
#         # Connect to database
#         self.db_manager.connect()
        
#     def on_created(self, event):
#         if not event.is_directory:
#             print(f"New file detected: {event.src_path}")
#             try:
#                 self.claim_processor.process_file(Path(event.src_path))
#             except Exception as e:
#                 print(f"Error processing file {event.src_path}: {str(e)}")

#     def on_deleted(self, event):
#         if not event.is_directory:
#             print(f"File deleted: {event.src_path}")

#     def __del__(self):
#         # Cleanup when the handler is destroyed
#         if hasattr(self, 'db_manager'):
#             self.db_manager.disconnect()

# def monitor_directory():
#     os.makedirs(WATCH_DIRECTORY, exist_ok=True)
#     event_handler = DocumentHandler()
#     observer = Observer()
#     observer.schedule(event_handler, WATCH_DIRECTORY, recursive=True)

#     print(f"Starting to monitor directory: {WATCH_DIRECTORY}")
#     observer.start()
#     try:
#         while True:
#             time.sleep(1)
#     except KeyboardInterrupt:
#         print("\nStopping directory monitoring...")
#         observer.stop()
#     observer.join()

# def main():
#     try:
#         monitor_directory()
#     except Exception as err:
#         print(f"Error in monitor_directory: {err}")
#         raise

# if __name__ == "__main__":
#     main()

from pathlib import Path
from configparser import ConfigParser
from file_processor import (
    ConfigurationManager, 
    DatabaseManager, 
    OCRProcessor, 
    ClaimProcessor
)
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time
import os
import logging
from typing import List
import shutil
from datetime import datetime

# Custom Exception Classes
class ClaimProcessingError(Exception):
    """Base exception class for claim processing errors."""
    pass

class OCRError(ClaimProcessingError):
    """Exception raised for errors during OCR processing."""
    pass

class ValidationError(ClaimProcessingError):
    """Exception raised for file validation errors."""
    pass

class ProcessingError(ClaimProcessingError):
    """Exception raised for general processing errors."""
    pass

class DatabaseError(ClaimProcessingError):
    """Exception raised for database-related errors."""
    pass


WATCH_DIRECTORY = "D:\\psl\\ClaimProcessing\\incoming_files"
FAILED_DIRECTORY = "D:\\psl\\ClaimProcessing\\failed_files"


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('claim_processing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DocumentHandler(FileSystemEventHandler):
    def __init__(self):
        # Load configuration and initialize components
        self.db_config, self.dir_config = ConfigurationManager.load_config()
        self.db_manager = DatabaseManager(self.db_config)
        self.ocr_processor = OCRProcessor(self.dir_config.tesseract_cmd)
        self.claim_processor = ClaimProcessor(
            self.db_manager, 
            self.ocr_processor, 
            self.dir_config
        )

        # Create required directories
        self._create_required_directories()

        # Connect to database
        self.db_manager.connect()
        
        # Process any existing files in the directory
        self.process_existing_files()

    def _create_required_directories(self):
        """Create all required directories if they don't exist."""
        directories = [
            WATCH_DIRECTORY,
            FAILED_DIRECTORY,
            Path(FAILED_DIRECTORY) / "ocr_failed",
            Path(FAILED_DIRECTORY) / "processing_failed",
            Path(FAILED_DIRECTORY) / "validation_failed"
        ]
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
            
    def move_to_failed(self, file_path: Path, error_type: str, error_message: str):
        """
        Move failed file to appropriate failed directory with error information.
        
        Args:
            file_path: Path to the failed file
            error_type: Type of error (ocr, processing, validation)
            error_message: Detailed error message
        """
        try:
            # Create timestamp for unique file naming
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Determine failed directory based on error type
            failed_dir = Path(FAILED_DIRECTORY) / f"{error_type}_failed"
            
            # Create new filename with timestamp
            new_filename = f"{file_path.stem}_{timestamp}{file_path.suffix}"
            failed_path = failed_dir / new_filename
            
            # Move the file
            shutil.move(str(file_path), str(failed_path))
            
            # # Create error log file
            # error_log_path = failed_dir / f"{new_filename}.error.log"
            # with open(error_log_path, 'w') as f:
            #     f.write(f"Original File: {file_path}\n")
            #     f.write(f"Error Time: {datetime.now()}\n")
            #     f.write(f"Error Type: {error_type}\n")
            #     f.write(f"Error Message: {error_message}\n")
            
            logger.info(f"Moved failed file to {failed_path}")
            # logger.info(f"Created error log at {error_log_path}")
            
        except Exception as e:
            logger.error(f"Error moving failed file {file_path}: {str(e)}")
        
    # def process_existing_files(self):
    #     """Process any existing files in the watch directory."""
    #     existing_files = self.get_files_in_directory()
    #     if existing_files:
    #         logger.info(f"Found {len(existing_files)} existing files to process")
    #         for file_path in existing_files:
    #             self.process_single_file(file_path)

    # def get_files_in_directory(self) -> List[Path]:
    #     """Get list of files in watch directory."""
    #     watch_path = Path(WATCH_DIRECTORY)
    #     return [f for f in watch_path.glob('*') if f.is_file()]

    # def process_single_file(self, file_path: Path):
    #     """Process a single file with proper waiting and error handling."""
    #     try:
    #         # Wait for file to be completely written
    #         self._wait_for_file_ready(file_path)
            
    #         logger.info(f"Processing file: {file_path}")
    #         self.claim_processor.process_file(file_path)
    #         logger.info(f"Finished processing file: {file_path}")
            
    #     except Exception as e:
    #         logger.error(f"Error processing file {file_path}: {str(e)}")

    def process_existing_files(self):
        """Process any existing files in the watch directory."""
        existing_files = self.get_files_in_directory()
        if existing_files:
            logger.info(f"Found {len(existing_files)} existing files to process")
            for file_path in existing_files:
                self.process_single_file(file_path)

    def get_files_in_directory(self) -> List[Path]:
        """Get list of files in watch directory."""
        watch_path = Path(WATCH_DIRECTORY)
        return [f for f in watch_path.glob('*') if f.is_file()]

    def process_single_file(self, file_path: Path):
        """Process a single file with proper waiting and error handling."""
        try:
            # Wait for file to be completely written
            self._wait_for_file_ready(file_path)
            
            logger.info(f"Processing file: {file_path}")
            
            # Validate file before processing
            if not self._validate_file(file_path):
                raise ValueError("File validation failed")
            
            # Process the file
            self.claim_processor.process_file(file_path)
            logger.info(f"Finished processing file: {file_path}")
            
        except ValueError as e:
            logger.error(f"Validation error for file {file_path}: {str(e)}")
            self.move_to_failed(file_path, "validation", str(e))
        except OCRError as e:
            logger.error(f"OCR error for file {file_path}: {str(e)}")
            self.move_to_failed(file_path, "ocr", str(e))
        except Exception as e:
            logger.error(f"Processing error for file {file_path}: {str(e)}")
            self.move_to_failed(file_path, "processing", str(e))

    def _validate_file(self, file_path: Path) -> bool:
        """
        Validate file before processing.
        
        Args:
            file_path: Path to the file to validate
            
        Returns:
            bool: True if file is valid, False otherwise
        """
        try:
            # Check if file exists
            if not file_path.exists():
                raise ValueError("File does not exist")
                
            # Check if file is empty
            if file_path.stat().st_size == 0:
                raise ValueError("File is empty")
                
            # Check file extension
            if file_path.suffix.lower() not in ['.pdf', '.tiff', '.tif', '.jpg', '.jpeg', '.png']:
                raise ValueError("Invalid file format")
                
            return True
            
        except Exception as e:
            logger.error(f"Validation failed for {file_path}: {str(e)}")
            return False


    def _wait_for_file_ready(self, file_path: Path, timeout: int = 60):
        """
        Wait for file to be completely written to disk.
        
        Args:
            file_path: Path to the file
            timeout: Maximum time to wait in seconds
        """
        start_time = time.time()
        while True:
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Timeout waiting for file {file_path} to be ready")
            
            try:
                # Try to open file exclusively
                with open(file_path, 'rb') as f:
                    # Check if file size is stable
                    size1 = file_path.stat().st_size
                    time.sleep(1)
                    size2 = file_path.stat().st_size
                    
                    if size1 == size2:  # File size is stable
                        return
            except PermissionError:
                # File is still being written
                time.sleep(1)
                continue

    def on_created(self, event):
        """Handle new file creation event."""
        if not event.is_directory:
            file_path = Path(event.src_path)
            logger.info(f"New file detected: {file_path}")
            self.process_single_file(file_path)

    def on_deleted(self, event):
        """Handle file deletion event."""
        if not event.is_directory:
            logger.info(f"File deleted: {event.src_path}")

    def __del__(self):
        """Cleanup when the handler is destroyed."""
        if hasattr(self, 'db_manager'):
            self.db_manager.disconnect()

def monitor_directory():
    """Start monitoring the directory for new files."""
    os.makedirs(WATCH_DIRECTORY, exist_ok=True)
    event_handler = DocumentHandler()
    observer = Observer()
    observer.schedule(event_handler, WATCH_DIRECTORY, recursive=True)

    logger.info(f"Starting to monitor directory: {WATCH_DIRECTORY}")
    observer.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("\nStopping directory monitoring...")
        observer.stop()
    observer.join()

def main():
    """Main application entry point."""
    try:
        monitor_directory()
    except Exception as err:
        logger.error(f"Error in monitor_directory: {err}")
        raise

if __name__ == "__main__":
    main()

