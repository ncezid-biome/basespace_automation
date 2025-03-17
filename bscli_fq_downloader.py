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

# Define output directory for downloaded files
# OUTPUT_DIR = "/scicomp/groups-pure/OID/NCEZID/DFWED/EDLB/projects/CIMS/HMAS_pilot/Incoming/test"
OUTPUT_DIR = "/scicomp/groups-pure/OID/NCEZID/DFWED/EDLB/projects/CIMS/HMAS_pilot/dev/Incoming"
# STEP_MOTHUR_OUTPUT_DIR = '/scicomp/groups-pure/OID/NCEZID/DFWED/EDLB/projects/CIMS/HMAS_pilot/sm_outputs/test'
STEP_MOTHUR_OUTPUT_DIR = '/scicomp/groups-pure/OID/NCEZID/DFWED/EDLB/projects/CIMS/HMAS_pilot/dev/sm_outputs'
EXTENSION = "fastq.gz"
OLIGO_FILE = "/scicomp/groups-pure/OID/NCEZID/DFWED/EDLB/projects/CIMS/HMAS_pilot/step_mothur/HMAS-QC-Pipeline2/M3235_22_024.oligos"
LOG_FILE = "step_mothur_log"
STEP_MOTHUR = "/scicomp/groups-pure/OID/NCEZID/DFWED/EDLB/projects/CIMS/HMAS_pilot/step_mothur/HMAS-QC-Pipeline2/"
PIPELINE_RUN_LOG_FILE = "stepmothur_download.log"
SENT_TO = "qtl7@cdc.gov" # wwm8@cdc.gov"
SPHL_CODE_LOG = "sphl_code_log"
LOG_FILE_TMP = LOG_FILE + ".tmp"

# Initialize an empty dictionary
owner_code_dict = {}

# Read the file and populate the dictionary
with open(SPHL_CODE_LOG, "r") as file:
    for line in file:
        parts = line.strip().split("\t")  # Split by tab
        if len(parts) >= 3:  # Ensure there are at least 3 columns
            owner_id, _, code = parts  # Extract ownerId and code, skip owner_name
            owner_code_dict[owner_id] = code  


log_lock = threading.Lock()  # Lock for updating the run counter
# last_run_number = 99  # Default so the first assigned is 100
last_run_number = 0

# Configure logging
logging.basicConfig(
    filename=PIPELINE_RUN_LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

def get_next_run_id(sphl_code):
    """Safely retrieves and increments the highest runID in the log."""
    global last_run_number

    with log_lock:
        if os.path.exists(LOG_FILE):
            with open(LOG_FILE, "r") as f:
                reader = csv.reader(f, delimiter="\t")
                for row in reader:
                    if len(row) < 5:
                        continue  # Skip malformed lines
                    #expected format: project_name, project_id, run_id, owner_id, timestamp
                    run_id = row[2]  # Extract runID (3th column)
                    match = re.search(r'(\d+)$', run_id)  # Extract numeric part
                    if match:
                        try:
                            last_run_number = max(last_run_number, int(match.group(1)))
                        except ValueError:
                            continue  # Skip invalid entries

        # Increment and generate new runID
        last_run_number += 1
        new_run_id = f"HMAS_{last_run_number:03}_{sphl_code}" #padding as 3-digits

    return new_run_id

def run_command(command):
    """Executes a shell command and returns the output."""
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {command}\n{e.stderr}")
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
        # projects[project_id] = project_name
        owner_id = row["UserOwnedBy.Id"]
        owner_name = row["UserOwnedBy.Name"]
        projects[project_id] = (project_name, owner_id, owner_name)

    return projects


def has_project_been_downloaded(project_name, project_id): 
    """Checks if the project with the given ID has already been logged in step_mothur_log."""
    if not os.path.exists(LOG_FILE):
        return False

    with open(LOG_FILE, "r") as f:
        downloaded_projects = {(line.strip().split("\t")[0], line.strip().split("\t")[1]) for line in f.readlines() if "\t" in line}

    return (project_name, project_id) in downloaded_projects


def log_downloaded_project(project_name, project_id, run_id, owner_id):
    """Logs the downloaded project name and timestamp to step_mothur_log."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a") as f:
        f.write(f"{project_name}\t{project_id}\t{run_id}\t{owner_id}\t{timestamp}\n")

def delete_logged_project(project_name, project_id, run_id, owner_id):
    """Safely deletes a record from the log file while handling concurrent access."""
    with log_lock:  # Use the existing lock to ensure thread safety
        found = False
        try:
            with open(LOG_FILE, "r") as infile, open(LOG_FILE_TMP, "w") as outfile:
                for line in infile:
                    fields = line.strip().split("\t")
                    if len(fields) >= 4 and fields[:4] == [project_name, project_id, run_id, owner_id]:
                        found = True
                        continue  # Skip writing this line (deleting it)
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
        print(f"Error: Download failed for project {project_name}.")
        return False

    return True

def download_and_run_stepmothur(project_name, owner_id, owner_name, project_id):

    #check if there is already a code for the owner in SPHL_CODE_LOG
    if owner_id in owner_code_dict:
        sphl_code = owner_code_dict[owner_id]
    else:
        send_mail = f"""mail -s "Test Subject {project_name}({project_id})"  {SENT_TO} <<EOF
                            Hello,

                            This is a test email.

                            ***WARNING***
                            {owner_id}/{owner_name} doesn't have a HMAS code in the {SPHL_CODE_LOG} file!
                            Hence {project_name}({project_id}) was not processed.

                            Best,
                            STEP_MOTHUR from CIMS 
                            EOF
                            """
        run_command(send_mail)
        logging.error(f"{owner_id}/{owner_name} doesn't have a HMAS code in the {SPHL_CODE_LOG} file!")
        return

    run_id = get_next_run_id(sphl_code)
    print(f"Downloading project {project_name}...")
    success = download_project_files(project_id, project_name, run_id)

    # project_dir = os.path.abspath(os.path.join(OUTPUT_DIR, f"{project_name}_{project_id}"))
    project_dir = os.path.join(OUTPUT_DIR, f"{run_id}_{project_name}")
    # output = os.path.join(STEP_MOTHUR_OUTPUT_DIR,f"{project_name}_{project_id}")
    output = os.path.join(STEP_MOTHUR_OUTPUT_DIR,f"{run_id}")
    current_dir = os.getcwd()
    command = (f" cd {STEP_MOTHUR} && "
               f"nextflow run hmas2.nf --primer {OLIGO_FILE}  "
               f"--reads {project_dir} "
               f"--outdir {output} && stty erase ^H && stty erase ^? && "
               f"cd {current_dir}")

    if success:
        log_downloaded_project(project_name, project_id, run_id, owner_id)
        result = subprocess.run(command, shell=True)
        if result.returncode == 0:  # Check if the command was successful
            print(f"step_mothur is successful for {project_name}. Running {command}...")
            # log_downloaded_project(project_name, project_id, run_id, owner_id)

            send_mail = f"""mail -s "Test Subject {project_name}({project_id})" -a {STEP_MOTHUR_OUTPUT_DIR}/{run_id}*/*.html  {SENT_TO} <<EOF
                        Hello,

                        This is a test email.
                        {project_name}({project_id}) has been successfully downloaded at:
                        {OUTPUT_DIR}
                        and step_mothur report can be found at:
                        {STEP_MOTHUR_OUTPUT_DIR} , with run_id starting with {run_id}

                        Best,
                        STEP_MOTHUR from CIMS 
                        EOF
                        """
            run_command(send_mail)
        else:
            print(f"step_mothur  failed for {project_name}. Skipping log update.")
            ####  code here to delete the record ! log_downloaded_project(project_name, project_id, run_id, owner_id)
            if not delete_logged_project(project_name, project_id, run_id, owner_id):
                logging.error(f"Either {LOG_FILE} does not exist or ({project_name}, {project_id}, {run_id}, {owner_id}) is not in the record")
    else:
        print(f"Download failed for {project_name}. Skipping execution of {command}.")


# Function to handle downloading and processing
def process_project(project_id, project_name, owner_id, owner_name):
    if has_project_been_downloaded(project_name, project_id):
        logging.info(f"Project {project_name} already downloaded. Skipping.")
        return

    logging.info(f"Starting download for project {project_name} (ID: {project_id})")
    try:
        download_and_run_stepmothur(project_name, owner_id, owner_name, project_id)
        logging.info(f"Successfully downloaded and processed {project_name} (ID: {project_id})")
    except Exception as e:
        logging.error(f"Error processing project {project_name} (ID: {project_id}): {e}")

# Number of concurrent downloads (adjust as needed)
MAX_WORKERS = 3  

def main():
    """this script can be run as: 
    python bscli_fq_downloader.py, this will check for all new projects under your account, and will
    download and run step_mothur on those new projects (once that's successfully completed, the project
    name will be written into LOG_FILE, and those projects will not be NEW anymore)
    or it be run as:
    python bscli_fq_downloader.py --project-id 00000, this will be checked to see if it's a valid project ID
    under your account and if it's a 'new' project. If yes to both, it will be downloaded and run through 
    step_mothur"""
    parser = argparse.ArgumentParser(description="Download FASTQ files from BaseSpace projects.")
    parser.add_argument(
        "--project-id",
        help="Optional Project ID to download. If not provided, the script checks for new projects automatically.",
    )
    args = parser.parse_args()

    print("Fetching available projects...")
    projects = get_available_projects()

    if not projects:
        print("No projects found or failed to retrieve projects.")
        return

    # If a specific project ID is provided
    if args.project_id:
        if args.project_id not in projects:
            print(f"Error: Project ID {args.project_id} not found.")
            return
        
        project_name, owner_id, owner_name = projects[args.project_id]

        if has_project_been_downloaded(project_name, args.project_id):
            print(f"Project {project_name}/{args.project_id} was already downloaded. Skipping.")
            return
        
        download_and_run_stepmothur(project_name, owner_id, owner_name, args.project_id)

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


if __name__ == "__main__":
    main()
