# Environment Configuration Files Setup
(delete this section once set up is complete)
1. Rename each of the files with the relevant line of business (LoB) name
2. Complete the yaml file for each environment (dev, qa, prd); all of these files should be present on each of the long-lived branches that correspond to each of those environments
       - Any occurrence of \<domain> should be replaced with the domain for the repo/GCP project (e.g. parallon); recommendation is to use CTRL+F while editing the file to use the find and replace option
    - Any occurrence of \<subDomain> should be deleted and replaced with the relevant subDomain using lowercase letters (e.g in the case of RA then \<subDomain> would be replaced by ra); recommendation is to use CTRL+F while editing the file to use the find and replace option
    - Other instructions contained by <> should also be deleted and replaced with the information described
    - Delete any other instructions added as comments once completed
    - Delete any extraneous variables that are not applicable
    - Add any additional variables that are needed

(end of section to delete upon completing setup)

# Environment Configuration Files

These files set up variables that can be used by other scripts in the repo. The majority of these variables are environment specific (dev, qa, prd), so a configuration file is created for each environment to align with this. All files should be present on each of the long-lived branches of the repo, and any updates must be promoted accordingly to avoid merge conflicts and unexpected behavior.
