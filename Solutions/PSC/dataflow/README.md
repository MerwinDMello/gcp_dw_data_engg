# Dataflow folder
This folder contains all content related to deployment of Dataflow jobs.

**Each Dataflow job should have its own subfolder**, named in such a way to logically relate to the function of the job, and a folder named src should be saved within each job subfolder. An associated config file should also be placed in the config folder. **It is very important to match this structure in order for the dataflow workflow to function properly.**

## Folder structure:

- dataflow
    - python
        - job-specific folder
            - config
                - job-specific yaml config file, see example for proper setup
            - src
                - All associated directories/files
                - At minimum: Dockerfile, main.py, metadata.json, requirements.txt
    - java
        - job-specific folder
            - config
                - job-specific yaml config file, see example for proper setup
            - src
                - All associated directories/files
    -yaml
        - job-specific folder
            - config
                - job-specific yaml config file, see example for proper setup
            - src
                - pipeline.yaml (**MUST have this exact name**)


## Job-Specific Config Files for Dataflow Jobs

**Each dataflow job in the repo needs to have its own config file** placed within the config folder for that job and specifying the variables noted.  If it is a streaming job, additional variables will be needed to run the deployment. See the example config in the example-df directories for more details, and follow it **EXACTLY**.