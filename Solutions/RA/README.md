# Initial Repo Setup 
(delete this section once setup is completed)
1.  Review the READMEs within the following folders as they provide guidance on setting up their contents for use:
    - workflows
    - dags
    - docs
    - environments
    - terraform
2. Starting on the dev branch, create a feature branch and make updates to the following key areas:
    - All environment config files (update file name, define variables)
    - The README at the root of the folder to make sure it correctly defines any particular elements of branch strategy, processes, and conventions for this repo
    - The CODEOWNERS file in the docs folder to define code owners for the repository
    - If ready to do so, go ahead and begin adding secrets as defined in the terraform README (this can come later if not immediately needed)
3. Merge the changes from the dev branch into the qa branch
4. Merge the changes from the qa branch into the prd branch
5. Review branch rules for all 3 branches to ensure they align with your desired processes/requirements
7. Begin pushing code!
8. Once you have one or more Python scripts committed to the repo, set up and enable CodeQL code scanning. This is found under Settings > Code security and analysis within the "Code scanning section". Select **Set up** and click **Default**. You can find a walkthrough of these steps and more information about CodeQL here:    
https://docs.github.com/en/code-security/code-scanning/enabling-code-scanning/configuring-default-setup-for-code-scanning
        
(this is the end of the section to delete once setup is complete)

# Repo Overview

This repository contains the code for ingestion and integration of data for edwra, part of the Parallon HIN domain. Each major folder contains a README with additional context for its contents.

## Conventions and Standards for this Repository

A full description of conventions and standards for work in this repository can be found on the [HIN GitHub SharePoint site](https://hcahealthcare.sharepoint.com/sites/CORP-TeradataMigration/SitePages/HIN-GitHub.aspx)   

Topics include:
- Branching and Merging workflow (this workflow applies following completion of initial migration work)
- Branch naming conventions
- Commit message conventions
- PR message conventions
- Release strategy and release naming standards

Any specific variations from the general HIN GitHub standards/conventions particular to this subdomain's repository should be detailed in this section.

## Repository & Code Documentation

- READMEs should be updated in conjunction with any major additions/updates to the primary folders of the repo
    - Standard sections: Overview, How it Works, Uses
- Comments should be used thoughtfully and regularly throughout code to allow others to more readily orient themselves to the contents of script

## GitHub Resources and References
A wide variety of resources to establish a [strong foundational understanding of GitHub](https://hcahealthcare.sharepoint.com/sites/CORP-TeradataMigration/SitePages/Getting-Started-with-GitHub.aspx) as well as guides for [everyday GitHub processes](https://hcahealthcare.sharepoint.com/sites/CORP-TeradataMigration/SitePages/Common-GitHub-Processes.aspx) and [repository maintenance processes](https://hcahealthcare.sharepoint.com/sites/CORP-TeradataMigration/SitePages/More-technical.aspx?Mode=Edit) are also available on the [HIN GitHub SharePoint site](https://hcahealthcare.sharepoint.com/sites/CORP-TeradataMigration/SitePages/HIN-GitHub-Resources-Processes.aspx).
