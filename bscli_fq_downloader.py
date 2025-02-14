#!/usr/bin/env python

import os
import argparse
import subprocess
import csv
import io
from datetime import datetime
import concurrent.futures
import logging

# Define output directory for downloaded files
# OUTPUT_DIR = "/scicomp/groups-pure/OID/NCEZID/DFWED/EDLB/projects/CIMS/HMAS_pilot/Incoming/test"
OUTPUT_DIR = "/scicomp/groups-pure/OID/NCEZID/DFWED/EDLB/projects/CIMS/HMAS_pilot/Incoming"
# STEP_MOTHUR_OUTPUT_DIR = '/scicomp/groups-pure/OID/NCEZID/DFWED/EDLB/projects/CIMS/HMAS_pilot/sm_outputs/test'
STEP_MOTHUR_OUTPUT_DIR = '/scicomp/groups-pure/OID/NCEZID/DFWED/EDLB/projects/CIMS/HMAS_pilot/sm_outputs'
EXTENSION = "fastq.gz"
OLIGO_FILE = "/scicomp/groups-pure/OID/NCEZID/DFWED/EDLB/projects/CIMS/HMAS_pilot/step_mothur/HMAS-QC-Pipeline2/M3235_22_024.oligos"
LOG_FILE = "step_mothur_log"
STEP_MOTHUR = "/scicomp/groups-pure/OID/NCEZID/DFWED/EDLB/projects/CIMS/HMAS_pilot/step_mothur/HMAS-QC-Pipeline2/"
PIPELINE_RUN_LOG_FILE = "stepmothur_download.log"
SENT_TO = "qtl7@cdc.gov" # wwm8@cdc.gov"


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
    command = "bs list projects -f csv"
    output = run_command(command)

    if not output:
        return {}

    projects = {}

    # Parse CSV output
    csv_reader = csv.DictReader(io.StringIO(output))
    for row in csv_reader:
        project_id = row["Id"]
        project_name = row["Name"]
        projects[project_id] = project_name

    return projects


def has_project_been_downloaded(project_name, project_id): 
    """Checks if the project with the given ID has already been logged in step_mothur_log."""
    if not os.path.exists(LOG_FILE):
        return False

    with open(LOG_FILE, "r") as f:
        downloaded_projects = {(line.strip().split("\t")[0], line.strip().split("\t")[1]) for line in f.readlines() if "\t" in line}

    return (project_name, project_id) in downloaded_projects


def log_downloaded_project(project_name, project_id):
    """Logs the downloaded project name and timestamp to step_mothur_log."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a") as f:
        f.write(f"{project_name}\t{project_id}\t{timestamp}\n")


def download_project_files(project_id, project_name):
    """Downloads FASTQ files for a given project and verifies success.
        bscli is smart, it checks for all fastq files of the given project are already D/Led
        and it will only download those were not previously D/Led
        it gives you: WARNING: downloaded 2/96 files (existing: 94) ! -- still returncode = 0
    """
    project_dir = os.path.join(OUTPUT_DIR, f"{project_name}_{project_id}")
    os.makedirs(project_dir, exist_ok=True)

    command = f"bs download project -i {project_id} -o {project_dir} --extension={EXTENSION}"
    print(f"Executing: {command}")

    # Run the command and check if it was successful
    result = subprocess.run(command, shell=True)

    if result.returncode != 0:
        print(f"Error: Download failed for project {project_name}.")
        return False

    return True

def download_and_run_stepmothur(project_name, project_id):

    print(f"Downloading project {project_name}...")
    success = download_project_files(project_id, project_name)

    project_dir = os.path.abspath(os.path.join(OUTPUT_DIR, f"{project_name}_{project_id}"))
    output = os.path.join(STEP_MOTHUR_OUTPUT_DIR,f"{project_name}_{project_id}")
    current_dir = os.getcwd()
    command = (f" cd {STEP_MOTHUR} && "
               f"nextflow run hmas2.nf --primer {OLIGO_FILE}  "
               f"--reads {project_dir} "
               f"--outdir {output} && stty erase ^H && stty erase ^? && "
               f"cd {current_dir}")

    if success:
        result = subprocess.run(command, shell=True)
        if result.returncode == 0:  # Check if the command was successful
            print(f"step_mothur is successful for {project_name}. Running {command}...")
            log_downloaded_project(project_name, project_id)

            send_mail = f"""mail -s "Test Subject {project_name}({project_id})" -a {STEP_MOTHUR_OUTPUT_DIR}/{project_name}*/multiqc_report*.html  {SENT_TO} <<EOF
                        Hello,

                        This is a test email.
                        {project_name}({project_id}) has been successfully downloaded at:
                        {OUTPUT_DIR}
                        and step_mothur report can be found at:
                        {STEP_MOTHUR_OUTPUT_DIR}

                        Best,
                        STEP_MOTHUR from CIMS 
                        EOF
                        """
            run_command(send_mail)
        else:
            print(f"step_mothur  failed for {project_name}. Skipping log update.")
    else:
        print(f"Download failed for {project_name}. Skipping execution of {command}.")


# Configure logging
logging.basicConfig(
    filename=PIPELINE_RUN_LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Function to handle downloading and processing
def process_project(project_id, project_name):
    if has_project_been_downloaded(project_name, project_id):
        logging.info(f"Project {project_name} already downloaded. Skipping.")
        return

    logging.info(f"Starting download for project {project_name} (ID: {project_id})")
    try:
        download_and_run_stepmothur(project_name, project_id)
        logging.info(f"Successfully downloaded and processed {project_name} (ID: {project_id})")
    except Exception as e:
        logging.error(f"Error processing project {project_name} (ID: {project_id}): {e}")

# Number of concurrent downloads (adjust as needed)
MAX_WORKERS = 4  

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
        
        project_name = projects[args.project_id]

        if has_project_been_downloaded(project_name, args.project_id):
            print(f"Project {project_name}/{args.project_id} was already downloaded. Skipping.")
            return
        
        download_and_run_stepmothur(project_name, args.project_id)

        return

    # otherwise, download and run new projects simultaneously
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_project, project_id, project_name): project_name for project_id, project_name in projects.items()}

    # Wait for all tasks to complete
    for future in concurrent.futures.as_completed(futures):
        project_name = futures[future]
        try:
            future.result()  # Check for errors
        except Exception as e:
            logging.error(f"Unhandled error in processing {project_name}: {e}")


if __name__ == "__main__":
    main()
