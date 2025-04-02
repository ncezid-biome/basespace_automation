#!/usr/bin/env python

import logging
import subprocess
import schedule
import time
import datetime
import argparse

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
    
# Function to validate time format (HH:MM)
def valid_time(s):
    try:
        datetime.datetime.strptime(s, "%H:%M")
        return s
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid time format: {s}. Use HH:MM (24-hour format).")

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Schedule BaseSpace download at specified times.")
parser.add_argument("times", nargs="+", type=valid_time, help="Scheduled times in HH:MM format (24-hour).")
args = parser.parse_args()

# Schedule the task to run at 11 PM and 5 AM every day
# schedule.every().day.at("22:16").do(run_download_command)
# schedule.every().day.at("10:19").do(run_download_command)

# Schedule tasks based on command-line input
for t in args.times:
    schedule.every().day.at(t).do(run_download_command)
    logging.info(f"Scheduled download at {t}")

# Keep the script running to handle the scheduled tasks
while True:
    schedule.run_pending()  # Check for pending tasks to run
    time.sleep(60)  # Wait for 1 minute before checking again
