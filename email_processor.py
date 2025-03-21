import imaplib
import email
import os
from email.header import decode_header
import logging

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Email configuration
EMAIL_CONFIG = {
    "EMAIL_USER": "rajnext25@gmail.com",
    "EMAIL_PASS": "ojxm wwdt ojmr ffdj",  # Use app-specific password for Gmail
    "IMAP_SERVER": "imap.gmail.com",  # or "outlook.office365.com" for Outlook
    "SAVE_PATH": "D:\\psl\\ClaimProcessing\\incoming_files"
}

def create_imap_connection():
    """Create and return an IMAP connection"""
    try:
        mail = imaplib.IMAP4_SSL(EMAIL_CONFIG["IMAP_SERVER"])
        mail.login(EMAIL_CONFIG["EMAIL_USER"], EMAIL_CONFIG["EMAIL_PASS"])
        return mail
    except imaplib.IMAP4.error as e:
        logger.error(f"IMAP connection failed: {str(e)}")
        raise

def get_filename_from_part(part):
    """Extract filename from email part, handling encoding"""
    filename = part.get_filename()
    if filename:
        # Decode filename if needed
        decoded_parts = decode_header(filename)
        filename = decoded_parts[0][0]
        if isinstance(filename, bytes):
            filename = filename.decode()
    return filename

def save_attachment(part, filename):
    """Save attachment to disk"""
    try:
        filepath = os.path.join(EMAIL_CONFIG["SAVE_PATH"], filename)
        # Ensure directory exists
        os.makedirs(EMAIL_CONFIG["SAVE_PATH"], exist_ok=True)
        
        with open(filepath, "wb") as f:
            f.write(part.get_payload(decode=True))
        logger.info(f"Saved attachment: {filepath}")
        return True
    except Exception as e:
        logger.error(f"Error saving attachment {filename}: {str(e)}")
        return False

def fetch_email_attachments():
    """Main function to fetch email attachments"""
    try:
        mail = create_imap_connection()
        
        # Select inbox
        mail.select("inbox")
        
        # Search for unread messages
        status, messages = mail.search(None, 'UNSEEN')
        if status != 'OK':
            logger.error("No messages found!")
            return
        
        message_numbers = messages[0].split()
        logger.info(f"Found {len(message_numbers)} unread messages")
        
        for num in message_numbers:
            try:
                status, msg_data = mail.fetch(num, '(RFC822)')
                if status != 'OK':
                    continue
                    
                email_body = msg_data[0][1]
                email_message = email.message_from_bytes(email_body)
                
                # Process attachments
                for part in email_message.walk():
                    if part.get_content_maintype() == 'multipart':
                        continue
                    if part.get('Content-Disposition') is None:
                        continue
                        
                    filename = get_filename_from_part(part)
                    if filename:
                        save_attachment(part, filename)
                
            except Exception as e:
                logger.error(f"Error processing message {num}: {str(e)}")
                continue
                
    except Exception as e:
        logger.error(f"Error in fetch_email_attachments: {str(e)}")
    finally:
        try:
            mail.logout()
        except:
            pass
