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

WATCH_DIRECTORY = "D:\\psl\\ClaimProcessing\\incoming_files"

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
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
        # Connect to database
        self.db_manager.connect()
        
        # Process any existing files in the directory
        self.process_existing_files()
        
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
            self.claim_processor.process_file(file_path)
            logger.info(f"Finished processing file: {file_path}")
            
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {str(e)}")

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

