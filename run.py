import threading
import time
import monitor_directory
import email_processor

def start_folder_monitor():
    monitor_directory.main()

def start_email_monitor():
    while True:
        email_processor.fetch_email_attachments()
        time.sleep(60)  # Wait for 60 seconds between email checks

if __name__ == "__main__":
    # Start the monitoring threads
    folder_monitor = threading.Thread(target=start_folder_monitor, daemon=True)
    email_monitor = threading.Thread(target=start_email_monitor, daemon=True)
    
    folder_monitor.start()
    email_monitor.start()
    
    try:
        # Keep the main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")
