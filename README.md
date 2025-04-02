# template

The purpose of this repository is to serve as a nice template to align with the values in DFWED.

## Overview
 
The BaseSpace automation pipeline scans your BaseSpace account for all available project data sets. It checks your local `step_mothur_log` file to determine if a specific project data set has been downloaded and/or analyzed by `step_mothur`. It reads all required parameters from a `config file (config.ini)`  
Since the pipeline will run `step_mothur` pipeline, you need to set up the environment properly, please refer to <a href="https://github.com/ncezid-biome/HMAS-QC-Pipeline2" target="_blank">step_mothur github repo</a>.   
And if this is the first time you're running BaseSpace automation pipeline, you will also need to install a python **schedule module** , by `pip install schedule `(so the pipeline runs periodically at given time slots).  


## Main Scripts

### 1. `bscli_fq_downloader.py`

This script checks for new projects under your BaseSpace account, downloads them, and runs `step_mothur` on those new projects.

#### Usage:

```sh
python bscli_fq_downloader.py -c config.ini
```

- This will check for all new projects under your BaseSpace account.
- Successfully processed projects are logged in `step_mothur_log`, ensuring they are not processed again.

 

#### note:  
-  record in `step_mothur_log` is formatted as: project_name, project_id, run_id, owner_id, timestamp. 
-  `run_id` is formatted as: HMAS_{last_run_number: 3-digit padding}_{sphl_code}, `sphl_code` need to exist in `SPHL_CODE_LOG`, or the script will stop and notify you thourh an email.  
-  there are a few configuration variables with default values in the beginning of the script, which need to be updated when running in a different environment.  


### Prerequisites

Before running these scripts, ensure:

1. You have installed **BaseSpace Sequence Hub CLI** ([documentation](https://developer.basespace.illumina.com/docs/content/documentation/cli/cli-overview)).
2. You have authenticated your BaseSpace account by running:
   ```sh
   bs auth
   ```
3. You might need these info for the authentication.  

```sh
Step-mothur-downloader v1.0.0

Client Id
99290fe834f241ff8d19df9edbc4f250

Client Secret
8624c0cf939044a3a72c1b7d397f46ff

Access Token
902ebaa7c85342fe9f0decb6ee68bf4a

Or use CIMS basespace account credentials to log in  
```

## Notices

### Public Domain Notice

This repository constitutes a work of the United States Government and is not subject to domestic copyright protection under 17 USC § 105. This repository is in the public domain within the United States, and copyright and related rights in the work worldwide are waived through the [CC0 1.0 Universal public domain dedication](https://creativecommons.org/publicdomain/zero/1.0/). All contributions to this repository will be released under the CC0 dedication. By submitting a pull request you are agreeing to comply with this waiver of copyright interest.

### License Standard Notice

### Privacy Notice

This repository contains only non-sensitive, publicly available data and information. All material and community participation is covered by the [Disclaimer](https://github.com/CDCgov/template/blob/master/DISCLAIMER.md) and [Code of Conduct](https://github.com/CDCgov/template/blob/master/code-of-conduct.md). For more information about CDC's privacy policy, please visit [http://www.cdc.gov/other/privacy.html](https://www.cdc.gov/other/privacy.html).

### Contributing Notice

Anyone is encouraged to contribute to the repository by [forking](https://help.github.com/articles/fork-a-repo) and submitting a pull request. (If you are new to GitHub, you might start with a [basic tutorial](https://help.github.com/articles/set-up-git).) By contributing to this project, you grant a world-wide, royalty-free, perpetual, irrevocable, non-exclusive, transferable license to all users under the terms of the [Apache Software License v2](http://www.apache.org/licenses/LICENSE-2.0.html) or later.

All comments, messages, pull requests, and other submissions received through CDC including this GitHub page may be subject to applicable federal law, including but not limited to the Federal Records Act, and may be archived. Learn more at [http://www.cdc.gov/other/privacy.html](http://www.cdc.gov/other/privacy.html).

### Records Management Notice

This repository is not a source of government records, but is a copy to increase collaboration and collaborative potential. All government records will be published through the [CDC web site](http://www.cdc.gov).

