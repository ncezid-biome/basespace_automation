#!/usr/bin/env python

import logging
import subprocess
import schedule
import time

# Function to run the BaseSpace download command
def run_download_command():
    command = "python bscli_fq_downloader.py"  # Replace with your actual script path
    result = subprocess.run(command, shell=True)
    logging.info(f"Done with: {command}, with result as: {result}")

# # Set up logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='[%(asctime)s] %(message)s',
#     datefmt='%d/%m/%Y %H:%M:%S',
# )

# Schedule the task to run at 11 PM and 5 AM every day
schedule.every().day.at("23:00").do(run_download_command)
schedule.every().day.at("11:16").do(run_download_command)

# Keep the script running to handle the scheduled tasks
while True:
    schedule.run_pending()  # Check for pending tasks to run
    time.sleep(60)  # Wait for 1 minute before checking again
