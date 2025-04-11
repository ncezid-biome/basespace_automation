#!/usr/bin/env python

import os
import argparse
import subprocess
import csv
import io
from datetime import datetime
import concurrent.futures
import logging
import threading
import re
import shutil
# import signal
import sys
import subprocess
import configparser
import schedule
import time
from filelock import FileLock

parser = argparse.ArgumentParser(description="Download FASTQ files from BaseSpace projects.")
parser.add_argument("-c", "--config", required=True, help="Path to the config file")
args = parser.parse_args()

config = configparser.ConfigParser()
config.read(args.config)

OUTPUT_DIR = config["Settings"]["OUTPUT_DIR"]
STEP_MOTHUR_OUTPUT_DIR = config["Settings"]["STEP_MOTHUR_OUTPUT_DIR"]
EXTENSION = config["Settings"]["EXTENSION"]
OLIGO_FILE = config["Settings"]["OLIGO_FILE"]
LOG_FILE = config["Settings"]["LOG_FILE"]
STEP_MOTHUR = config["Settings"]["STEP_MOTHUR"]
PIPELINE_RUN_LOG_FILE = config["Settings"]["PIPELINE_RUN_LOG_FILE"]
SENT_TO = config["Settings"]["SENT_TO"]
SPHL_CODE_LOG = config["Settings"]["SPHL_CODE_LOG"]
LOG_FILE_TMP = LOG_FILE + ".tmp"
last_run_number = int(config.get("Settings", "last_run_number", fallback="100"))

# log_lock = threading.Lock()  # Lock for updating the run counter
log_lock = FileLock(LOG_FILE + ".lock")  # Lock file to prevent concurrent access

SM_RUN_SUCCESS = "SM_PASS"

# Initialize an empty dictionary
'''
create a dictionary from the SPHL_CODE_LOG
owner_id, owner_name, state_code
30888858        CIMS EDLB       JO
21732711        ODH Lab OH
58960902        NCSLPH HMAS     NC
'''
owner_code_dict = {}

# step_mothur_pipeline_success = False  # Track if step_mothur pipeline ran successfully

# Read the file and populate the dictionary
with open(SPHL_CODE_LOG, "r") as file:
    for line in file:
        parts = line.strip().split("\t")  # Split by tab
        if len(parts) >= 3:  # Ensure there are at least 3 columns
            owner_id, _, code = parts  # Extract ownerId and code, skip owner_name
            owner_code_dict[owner_id] = code  


# Configure logging
logging.basicConfig(
    filename=PIPELINE_RUN_LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# # handler method to clean up the matching record in the LOG_FILE, upon receiving
# # system interuption signal
# def create_cleanup_handler(project_name, project_id, run_id, owner_id):
#     def cleanup_and_exit(signum, frame):
#         if not step_mothur_pipeline_success:
#             logging.error("System shutdown detected. Cleaning up...")
#             update_logged_project(project_name, project_id, run_id, owner_id)
#         sys.exit(1)

#     return cleanup_and_exit

def get_next_run_id(sphl_code):
    """Safely retrieves and increments the highest runID in the log."""
    global last_run_number

    with log_lock:
        if os.path.exists(LOG_FILE):
            with open(LOG_FILE, "r") as f:
                reader = csv.reader(f, delimiter="\t")
                for row in reader:
                    if not row:  # skip empty lines
                        continue
                    #expected format: project_name, project_id, run_id, owner_id, timestamp, SM_STATUS
                    run_id = row[2]  # Extract runID (3th column)
                    match = re.search(r'(\d+)', run_id)  # Extract numeric part
                    if match:
                        try:
                            last_run_number = max(last_run_number, int(match.group(1)))
                        except ValueError:
                            logging.error(f"invalid entry in {LOG_FILE}, {ValueError}")
                            continue  # Skip invalid entries

        # increment and generate new runID
        last_run_number += 1
        new_run_id = f"HMAS_{last_run_number:03}_{sphl_code}" #padding as 3-digits

    return new_run_id

def run_command(command):
    """Executes a shell command and returns the output."""
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        logging.error(f"Error running command: {command}\n{e.stderr}")
        return None


def get_available_projects():
    """Retrieves the list of available projects using BaseSpace CLI (CSV format)."""
    # command = "bs list projects -f csv"
    command = "bs list project -F UserOwnedBy.Id -F UserOwnedBy.Name -F Id -F Name -f csv"
    output = run_command(command)

    if not output:
        return {}

    projects = {}

    # Parse CSV output
    csv_reader = csv.DictReader(io.StringIO(output))
    for row in csv_reader:
        project_id = row["Id"]
        project_name = row["Name"]
        owner_id = row["UserOwnedBy.Id"]
        owner_name = row["UserOwnedBy.Name"]
        projects[project_id] = (project_name, owner_id, owner_name)

    return projects


def has_project_been_downloaded(project_name, project_id): 
    """Checks if the project with the given ID has already been logged in step_mothur_log.
        return (True, None), if the project was already downloaded and SM run was successful (SM_RUN_SUCCESS)
        return (False, run_id), if the project was already downloaded but SM run failed (not status for SM_RUN)
        return (False, None), if the project was NOT downloaded
    """
    if not os.path.exists(LOG_FILE):
        logging.error(f"Can't find the {LOG_FILE}")
        sys.exit(1)

    # with open(LOG_FILE, "r") as f:
    #     downloaded_projects = {(line.strip().split('\t')[0], line.strip().split('\t')[1]) for line in f.readlines() if '\t' in line}

    with open(LOG_FILE, "r") as f:
            downloaded_projects = [
                (line.strip().split('\t')[0], line.strip().split('\t')[1], line.strip().split('\t')[2], line.strip().split('\t')[5] if len(line.strip().split('\t')) > 5 else None)
                for line in f.readlines() if '\t' in line
            ]
    
    for project in downloaded_projects:
        if project[0] == project_name and project[1] == project_id:
            # Check if the 6th column exists and is not None
            if project[3] is not None:
                return (True, None)  # if the project was already downloaded and SM run was successful (SM_RUN_SUCCESS)
            else:
                return (False, project[2])  # if the project was already downloaded but SM run failed, return the run_id
    
    return (False, None)  # If project is not found
    # return (project_name, project_id) in downloaded_projects


# def log_downloaded_project(project_name, project_id, run_id, owner_id):
#     """Logs the downloaded project name and timestamp to step_mothur_log."""
#     timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#     with open(LOG_FILE, "a") as f:
#         f.write(f"{project_name}\t{project_id}\t{run_id}\t{owner_id}\t{timestamp}\n")

def log_downloaded_project(project_name, project_id, run_id, owner_id):
    """Appends a log entry only if it doesn't already exist, using atomic file replacement."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    new_line = f"{project_name}\t{project_id}\t{run_id}\t{owner_id}\t{timestamp}\n"
    updated = False

    with log_lock:
        with open(LOG_FILE, "r") as infile, open(LOG_FILE_TMP, "w") as outfile:
            for line in infile:
                parts = line.strip().split("\t")
                if parts[:4] == [project_name, project_id, run_id, owner_id]:
                    outfile.write(new_line)  # update timestamp
                    updated = True
                else:
                    outfile.write(line)
        if not updated:
            with open(LOG_FILE_TMP, "a") as outfile:
                outfile.write(new_line)
        shutil.move(LOG_FILE_TMP, LOG_FILE)  # Atomic replacement


def update_logged_project(project_name, project_id, run_id, owner_id):
    """Safely updates a record from the log file while handling concurrent access."""
    with log_lock:  # Use the existing lock to ensure thread safety
        found = False
        try:
            #we 'update' it by copying the original log file over to a tmp file and skip
            #any matching records, then replace the original log file with the tmp file
            with open(LOG_FILE, "r") as infile, open(LOG_FILE_TMP, "w") as outfile:
                for line in infile:
                    fields = line.strip().split("\t")
                    if len(fields) >= 4 and fields[:4] == [project_name, project_id, run_id, owner_id]:
                        found = True
                        # continue  # Skip writing this line (deleting it)
                        fields.append('SM_PASS')
                        line = '\t'.join(fields) + '\n'
                    outfile.write(line)

            if found:
                shutil.move(LOG_FILE_TMP, LOG_FILE)  # Atomic file replacement
                return True  # Deletion was successful
            else:
                os.remove(LOG_FILE_TMP)  # Cleanup temp file
                return False  # No matching record found

        except FileNotFoundError:
            return False  # Log file doesn't exist


def download_project_files(project_id, project_name, run_id):
    """Downloads FASTQ files for a given project and verifies success.
        bscli is smart, it checks for all fastq files of the given project are already D/Led
        and it will only download those were not previously D/Led
        it gives you: WARNING: downloaded 2/96 files (existing: 94) ! -- still returncode = 0
    """
    project_name = re.sub(r'\s+', ' ', project_name.strip())  # Normalize spaces
    project_name = project_name.replace(" ", "_") 
    project_dir = os.path.join(OUTPUT_DIR, f"{run_id}_{project_name}")
    os.makedirs(project_dir, exist_ok=True)

    command = f"bs download project -i {project_id} -o {project_dir} --extension={EXTENSION}"
    print(f"Executing: {command}")

    # Run the command and check if it was successful
    result = subprocess.run(command, shell=True)

    if result.returncode != 0:
        logging.error(f"Error: Download failed for project {project_name}.")
        return False

    return True

def download_and_run_stepmothur(project_name, owner_id, owner_name, project_id, run_id=None):

    #check if there is already a code for the owner in SPHL_CODE_LOG
    if owner_id in owner_code_dict:
        sphl_code = owner_code_dict[owner_id]
    else:
        body = f"""Hello,

        ***WARNING***
        {owner_id}/{owner_name} doesn't have a HMAS code in the {SPHL_CODE_LOG} file!
        Hence {project_name}({project_id}) was not processed.

        Best,
        STEP_MOTHUR from CIMS 
        """
        send_mail = f'echo -e "{body}" | mail -s "{project_name}({project_id})" {SENT_TO}'
        run_command(send_mail)

        logging.error(f"{owner_id}/{owner_name} doesn't have a HMAS code in the {SPHL_CODE_LOG} file!")
        return False

    if not run_id: #if we don't have a run_id yet
        run_id = get_next_run_id(sphl_code)

    print(f"Downloading project {project_name}...")
    success = download_project_files(project_id, project_name, run_id)

    project_dir = os.path.join(OUTPUT_DIR, f"{run_id}_{project_name}")
    output = os.path.join(STEP_MOTHUR_OUTPUT_DIR,f"{run_id}")
    current_dir = os.getcwd()
    command = (f" cd {STEP_MOTHUR} && "
               f"nextflow run hmas2.nf --primer {OLIGO_FILE}  "
               f"--reads {project_dir} "
               f"--outdir {output} && stty erase ^H && stty erase ^? && "
               f"cd {current_dir}")

    if success:
        # try:
        # # Register signal handlers with correct parameters
        # cleanup_handler = create_cleanup_handler(project_name, project_id, run_id, owner_id)
        # signal.signal(signal.SIGTERM, cleanup_handler) #soft termination signal
        # signal.signal(signal.SIGINT, cleanup_handler) #Signal Interrupt Ctrl+C
        # signal.signal(signal.SIGHUP, cleanup_handler) #Signal Hangup, terminal disconnection

        log_downloaded_project(project_name, project_id, run_id, owner_id)
        result = subprocess.run(command, shell=True)
        if result.returncode == 0:  # Check if the command was successful
            if not update_logged_project(project_name, project_id, run_id, owner_id): #update log with SM_PASS
                logging.error(f"Either {LOG_FILE} does not exist or ({project_name}, {project_id}, {run_id}, {owner_id}) is not in the record")

            # global step_mothur_pipeline_success
            # step_mothur_pipeline_success = True
            print(f"step_mothur is successful for {project_name}. Running {command}...")

            body = f"""Hello,

            {project_name}({project_id}) has been successfully downloaded at:
            {OUTPUT_DIR}

            and step_mothur report can be found at:
            {STEP_MOTHUR_OUTPUT_DIR} , with run_id starting with {run_id}

            Best,
            STEP_MOTHUR from CIMS 
            """

            send_mail = f'echo -e "{body}" | mail -s "{project_name}({project_id})" -a {STEP_MOTHUR_OUTPUT_DIR}/{run_id}*/*.html {SENT_TO}'
            run_command(send_mail)

        else:
            print(f"step_mothur  failed for {project_name}. Skipping log update.")
            logging.error(f"step_mothur  failed for {project_name}. Skipping log update.")
            return False
    # finally:
    #     if not step_mothur_pipeline_success:
    #         if not update_logged_project(project_name, project_id, run_id, owner_id):
    #             logging.error(f"In finally, . Either {LOG_FILE} does not exist or ({project_name}, {project_id}, {run_id}, {owner_id}) is not in the record")
    #             logging.warning(f"if there is System shutdown error above, ignore the step_mothur_log error")

    else:
        logging.error(f"Download failed for {project_name}. Skipping execution of {command}.")
        return False

    return True

# Function to handle downloading and processing
def process_project(project_id, project_name, owner_id, owner_name):

    status, run_id = has_project_been_downloaded(project_name, project_id)
    if status: #already downloaded and SM run was successful
        logging.info(f"Project {project_name} already downloaded. Skipping.")
        return

    logging.info(f"Starting download for project {project_name} (ID: {project_id})")
    try:
        if download_and_run_stepmothur(project_name, owner_id, owner_name, project_id, run_id):
            logging.info(f"Successfully downloaded and processed {project_name} (ID: {project_id})")
    except Exception as e:
        # logging.error(f"Error processing project {project_name} (ID: {project_id}): {e}")
        logging.error(f"Error processing project {project_name} (ID: {project_id})", exc_info=True)

# Number of concurrent downloads (adjust as needed)
# switch off multi-threading because 'signal' only works in the main thread !
MAX_WORKERS = 3  

def main():
    """this script can be run as: 
    python bscli_fq_downloader.py -c config.ini
    it reads all required parameters from the config.ini file
    there are 2 ways to run the pipeline:
    1. if you give it an valid project_id in the config.ini file, the pipeline will check to see if it's a valid project ID
    under your account and if it's a 'new' project. If yes to both, the project will be downloaded and run through step_mothur
    2. leave the project_id empty and the pipeline will scan your basespace account, download/analyze all
    available projects
    """
    project_id = config["Settings"]["project_id"]

    print("Fetching available projects...")
    projects = get_available_projects()

    if not projects:
        print("No projects found or failed to retrieve projects.")
        return

    # If a specific project ID is provided
    if project_id:
        if project_id not in projects:
            logging.error(f"Error: Project ID {project_id} not found.")
            return
        
        project_name, owner_id, owner_name = projects[project_id]
        process_project(project_id, project_name, owner_id, owner_name)

        return

    # otherwise, download and run new projects simultaneously
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_project, project_id, project_name, owner_id, owner_name): project_name 
                   for project_id, (project_name, owner_id, owner_name) in projects.items()}

    # Wait for all tasks to complete
    for future in concurrent.futures.as_completed(futures):
        project_name = futures[future]
        try:
            future.result()  # Check for errors
        except Exception as e:
            logging.error(f"Unhandled error in processing {project_name}: {e}")

    # for project_id, (project_name, owner_id, owner_name) in projects.items(): 
    #     process_project(project_id, project_name, owner_id, owner_name)

if __name__ == "__main__":

    scheduled_time = config["Settings"].get("scheduled_time", "")

    # Split the string into a list of times
    time_list = scheduled_time.split()
    # Function to check and convert time_str into datetime object
    def valid_time(time_str):
        try:
            # Attempt to parse the time string
            datetime.strptime(time_str, "%H:%M")
            return time_str
        except ValueError:
            # Raise an error if the time format is invalid
            raise ValueError(f"Invalid time format: {time_str}. Expected HH:MM (24-hour).")

    # Convert each time to a datetime object, handling invalid times
    parsed_scheduled_time = []
    for time_str in time_list:
        try:
            parsed_time = valid_time(time_str)
            parsed_scheduled_time.append(parsed_time)
        except ValueError as e:
            logging.error(e) 
            sys.exit(1)

    # we have set up time slots to run the pipeline continuously
    if parsed_scheduled_time:

        # Schedule tasks based on command-line input
        for t in parsed_scheduled_time:
            print(f"running {os.path.basename(__file__)} at {t}")
            schedule.every().day.at(t).do(main)

        # Keep the script running to handle the scheduled tasks
        while True:
            schedule.run_pending()  # Check for pending tasks to run
            time.sleep(60)  # Wait for 1 minute before checking again

    else: #or it's just a one-off
        main()
