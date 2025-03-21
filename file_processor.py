import os
import re
import shutil
import logging
from pathlib import Path
from typing import Optional, Tuple, List
from dataclasses import dataclass
from datetime import datetime
import mysql.connector
from mysql.connector import Error as MySQLError
from pdf2image import convert_from_path
import pytesseract
import cv2
from configparser import ConfigParser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('claim_processor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class DatabaseConfig:
    """Database configuration container."""
    host: str
    user: str
    password: str
    database: str

@dataclass
class DirectoryConfig:
    """Directory paths configuration container."""
    incoming_dir: Path
    processed_dir: Path
    tesseract_cmd: str

class ConfigurationManager:
    """Manages application configuration."""
    
    @staticmethod
    def load_config(config_file: str = 'config.ini') -> Tuple[DatabaseConfig, DirectoryConfig]:
        """Load configuration from config file."""
        config = ConfigParser()
        
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"Configuration file {config_file} not found")
            
        config.read(config_file)
        
        db_config = DatabaseConfig(
            host=config['Database']['host'],
            user=config['Database']['user'],
            password=config['Database']['password'],
            database=config['Database']['database']
        )
        
        dir_config = DirectoryConfig(
            incoming_dir=Path(config['Directories']['incoming_dir']),
            processed_dir=Path(config['Directories']['processed_dir']),
            tesseract_cmd=config['OCR']['tesseract_cmd']
        )
        
        return db_config, dir_config

class DatabaseManager:
    """Manages database connections and operations."""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.connection = None
        self.cursor = None
        
    def connect(self) -> None:
        """Establish database connection and initialize schema."""
        try:
            self.connection = mysql.connector.connect(
                host=self.config.host,
                user=self.config.user,
                password=self.config.password,
                database=self.config.database
            )
            self.cursor = self.connection.cursor()
            logger.info("Database connection established successfully")
            self._initialize_database()
        except MySQLError as err:
            logger.error(f"Error connecting to database: {err}")
            raise
    
    def _initialize_database(self) -> None:
        """Initialize database schema if not exists."""
        try:
            # Create Claims table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS Claims (
                    ID INT AUTO_INCREMENT PRIMARY KEY,
                    ClaimNumber VARCHAR(50) NOT NULL UNIQUE,
                    DirectoryPath VARCHAR(255),
                    CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UpdatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )
            """)
            
            # Create ErrorFiles table without foreign key constraint
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS ErrorFiles (
                    ID INT AUTO_INCREMENT PRIMARY KEY,
                    FilePath VARCHAR(255) NOT NULL,
                    ClaimNumber VARCHAR(50),
                    ErrorMessage TEXT,
                    CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    ProcessedAt TIMESTAMP NULL,
                    IsUnmatchedClaim BOOLEAN DEFAULT FALSE
                )
            """)
            
            self.connection.commit()
            logger.info("Database schema initialized successfully")
        except MySQLError as err:
            logger.error(f"Error initializing database schema: {err}")
            self.connection.rollback()
            raise
            
    def disconnect(self) -> None:
        """Close database connection."""
        if self.connection and self.connection.is_connected():
            self.cursor.close()
            self.connection.close()
            logger.info("Database connection closed")
            
    def get_claims(self) -> List[Tuple[str, str]]:
        """Retrieve all claims from database."""
        try:
            self.cursor.execute("SELECT ClaimNumber, DirectoryPath FROM Claims")
            return self.cursor.fetchall()
        except MySQLError as err:
            logger.error(f"Error fetching claims: {err}")
            raise
            
    def update_claim_directory(self, claim_number: str, directory: str) -> None:
        """Update claim directory path."""
        try:
            self.cursor.execute(
                "UPDATE Claims SET DirectoryPath=%s WHERE ClaimNumber=%s",
                (directory, claim_number)
            )
            self.connection.commit()
        except MySQLError as err:
            logger.error(f"Error updating claim directory: {err}")
            self.connection.rollback()
            raise
            
    def log_error_file(self, file_path: str, error_message: str, claim_number: Optional[str] = None) -> None:
        """
        Log file processing errors to database.
        
        Args:
            file_path: Path to the file that caused the error
            error_message: Description of the error
            claim_number: Optional claim number if identified
        """
        try:
            # Check if claim number exists in Claims table
            is_unmatched = False
            if claim_number:
                self.cursor.execute(
                    "SELECT COUNT(*) FROM Claims WHERE ClaimNumber = %s",
                    (claim_number,)
                )
                if self.cursor.fetchone()[0] == 0:
                    is_unmatched = True
                    
            self.cursor.execute(
                """
                INSERT INTO ErrorFiles (
                    FilePath, 
                    ClaimNumber, 
                    ErrorMessage, 
                    IsUnmatchedClaim
                )
                VALUES (%s, %s, %s, %s)
                """,
                (str(file_path), claim_number, error_message, is_unmatched)
            )
            self.connection.commit()
            
            if is_unmatched:
                logger.warning(f"Unmatched claim number {claim_number} logged in ErrorFiles")
        except MySQLError as err:
            logger.error(f"Error logging file error: {err}")
            self.connection.rollback()
            raise
        
    def insert_test_claim(self, claim_number: str) -> None:
        """Insert a test claim for development purposes."""
        try:
            self.cursor.execute(
                "INSERT IGNORE INTO Claims (ClaimNumber) VALUES (%s)",
                (claim_number,)
            )
            self.connection.commit()
            logger.info(f"Test claim {claim_number} inserted successfully")
        except MySQLError as err:
            logger.error(f"Error inserting test claim: {err}")
            self.connection.rollback()
            raise


class OCRProcessor:
    """Handles OCR processing of documents."""
    
    def __init__(self, tesseract_cmd: str):
        pytesseract.pytesseract.tesseract_cmd = tesseract_cmd
        
    def extract_text_from_file(self, file_path: Path) -> str:
        """Extract text from PDF or image file."""
        try:
            if file_path.suffix.lower() == '.pdf':
                return self._process_pdf(file_path)
            elif file_path.suffix.lower() in ('.png', '.jpg', '.jpeg'):
                return self._process_image(file_path)
            else:
                raise ValueError(f"Unsupported file type: {file_path.suffix}")
        except Exception as err:
            logger.error(f"Error processing file {file_path}: {err}")
            raise
            
    def _process_pdf(self, file_path: Path) -> str:
        """Process PDF file and extract text."""
        images = convert_from_path(str(file_path))
        return ' '.join(pytesseract.image_to_string(img) for img in images)
        
    def _process_image(self, file_path: Path) -> str:
        """Process image file and extract text."""
        image = cv2.imread(str(file_path))
        return pytesseract.image_to_string(image)

class ClaimProcessor:
    """Main claim processing class."""
    
    def __init__(self, db_manager: DatabaseManager, ocr_processor: OCRProcessor, dir_config: DirectoryConfig):
        self.db_manager = db_manager
        self.ocr_processor = ocr_processor
        self.dir_config = dir_config
        
        # Ensure all required directories exist
        for directory in [
            self.dir_config.incoming_dir,
            self.dir_config.processed_dir,
            Path(str(self.dir_config.processed_dir) + "/no_claim")  # Create no_claim directory
        ]:
            directory.mkdir(parents=True, exist_ok=True)
    
    def _extract_potential_claim_number(self, text: str) -> Optional[str]:
        """
        Extract potential claim number from text that appears after claim-related keywords.
        Returns the first matching pattern or None if no pattern is found.
        The claim number must contain at least one digit.
        """
        # Convert text to lowercase for case-insensitive matching
        text_lower = text.lower()
        
        # Main pattern to find claim numbers that appear after claim-related keywords
        pattern = (
            r'(?:claim|claim\s+number|claim\s+id|claim\s+#)'  # Claim-related keywords
            r'\s*[:/#-]?\s*'  # Optional separators
            r'([a-zA-Z0-9](?:[a-zA-Z0-9-]*\d+[a-zA-Z0-9-]*|\d+))'  # Claim number with at least one digit
        )
        
        matches = re.search(pattern, text_lower, re.IGNORECASE)
        if matches:
            claim_number = matches.group(1).upper()
            # Additional check to ensure the claim number contains at least one digit
            if any(char.isdigit() for char in claim_number):
                logger.debug(f"Found potential claim number: {claim_number}")
                return claim_number
        
        return None


        
    def process_file(self, file_path: Path) -> None:
        """Process a single file and match it to a claim."""
        try:
            text = self.ocr_processor.extract_text_from_file(file_path)
            claims = self.db_manager.get_claims()
            
            # First try to find any potential claim number in the text
            potential_claim_number = self._extract_potential_claim_number(text)
            matched_claim = self._find_matching_claim(text, claims)
            
            if matched_claim:
                # Case 1: Claim exists in database
                self._handle_matched_claim(file_path, matched_claim)
            elif potential_claim_number:
                # Case 2: Claim number found in file but not in database
                unmatched_dir = self.dir_config.processed_dir / potential_claim_number
                unmatched_dir.mkdir(parents=True, exist_ok=True)
                
                # Move file to unmatched claim directory
                shutil.move(str(file_path), str(unmatched_dir / file_path.name))
                
                error_message = f"Claim number {potential_claim_number} found in file but not in database"
                self.db_manager.log_error_file(
                    str(unmatched_dir / file_path.name),
                    error_message,
                    potential_claim_number
                )
                logger.warning(f"File {file_path}: {error_message}")
            else:
                # Case 3: No claim number found
                no_claim_dir = self.dir_config.processed_dir / "no_claim"
                no_claim_dir.mkdir(parents=True, exist_ok=True)
                
                # Move file to no_claim directory
                shutil.move(str(file_path), str(no_claim_dir / file_path.name))
                
                error_message = "No claim number found in file"
                self.db_manager.log_error_file(
                    str(no_claim_dir / file_path.name),
                    error_message,
                    None
                )
                logger.warning(f"File {file_path}: {error_message}")
                
        except Exception as err:
            # Try to get claim number from either matched claim or potential claim
            claim_number = None
            if 'matched_claim' in locals() and matched_claim:
                claim_number = matched_claim[0]
            elif 'potential_claim_number' in locals() and potential_claim_number:
                claim_number = potential_claim_number
                
            logger.error(f"Error processing file {file_path}: {err}")
            self.db_manager.log_error_file(
                str(file_path),
                str(err),
                claim_number
            )

            
    def _find_matching_claim(self, text: str, claims: List[Tuple[str, str]]) -> Optional[Tuple[str, str]]:
        """
        Find matching claim from extracted text.
        Returns tuple of (claim_number, directory) if found, None otherwise.
        """
        for claim_number, directory in claims:
            if re.search(rf"\b{re.escape(claim_number)}\b", text, re.IGNORECASE):
                logger.info(f"Found matching claim number in database: {claim_number}")
                return claim_number, directory
        return None
        
    # def _handle_matched_claim(self, file_path: Path, matched_claim: Tuple[str, str]) -> None:
    #     """Handle matched claim processing."""
    #     try:
    #         claim_number, directory = matched_claim
            
    #         if not directory:
    #             directory = str(self.dir_config.processed_dir / claim_number)
    #             Path(directory).mkdir(parents=True, exist_ok=True)
    #             self.db_manager.update_claim_directory(claim_number, directory)
                
    #         shutil.move(str(file_path), str(Path(directory) / file_path.name))
    #         logger.info(f"File {file_path.name} moved to claim directory {directory}")
    #     except Exception as err:
    #         logger.error(f"Error handling matched claim: {err}")
    #         self.db_manager.log_error_file(
    #             str(file_path),
    #             f"Error handling matched claim: {err}",
    #             claim_number
    #         )
    #         raise

    def _handle_matched_claim(self, file_path: Path, matched_claim: Tuple[str, str]) -> None:
        """Handle matched claim processing."""
        try:
            claim_number, directory = matched_claim
            
            # If directory is not set in database, create it under processed_dir
            if not directory:
                directory = str(self.dir_config.processed_dir / claim_number)
                self.db_manager.update_claim_directory(claim_number, directory)
            
            # Ensure the directory exists
            dest_dir = Path(directory)
            dest_dir.mkdir(parents=True, exist_ok=True)
            
            # Create the full destination path
            dest_path = dest_dir / file_path.name
            
            # Ensure the source file exists before moving
            if not file_path.exists():
                raise FileNotFoundError(f"Source file {file_path} does not exist")
                
            # Move the file
            shutil.move(str(file_path), str(dest_path))
            logger.info(f"File {file_path.name} moved to claim directory {directory}")
            
        except Exception as err:
            logger.error(f"Error handling matched claim: {err}")
            self.db_manager.log_error_file(
                str(file_path),
                f"Error handling matched claim: {err}",
                claim_number if 'claim_number' in locals() else None
            )
            raise
 
    def monitor_incoming_files(self) -> None:
        """Monitor incoming directory for new files."""
        try:
            for file_path in self.dir_config.incoming_dir.iterdir():
                if file_path.is_file():
                    logger.info(f"Processing file: {file_path}")
                    self.process_file(file_path)
        except Exception as err:
            logger.error(f"Error monitoring incoming files: {err}")
            raise
