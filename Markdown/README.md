# Initial Repo Setup 
(delete this section once setup is completed)
1. Create a GitHub Team for each AAD group that should have some level of access to the repo (it is best practice to minimize creating teams of individuals as opposed to AAD groups). You may want to create a smaller admin team that will have administrative access to the repo.
2. Under Settings > Teams add and assign access level to teams and individuals (teams whenever possible) as appropriate
3. Review the READMEs within the following folders as they provide guidance on setting up their contents for use:
    - workflows
    - dags
    - docs
    - environments
    - terraform
4. Starting on the dev branch, create a feature branch and make updates to the following key areas:
    - All environment config files (update file name, define variables)
    - The Composer workflow (to align with whether the repository will support a single or multiple LoBs)
    - If the repo will support multiple LoBs, create subfolders for all LoBs within each of the folders noted in the dags folder README
    - The README at the root of the folder to make sure it correctly defines branch strategy, processes, and conventions
    - If ready to do so, go ahead and begin adding secrets as defined in the terraform README (this can come later if not immediately needed)
6. Merge the changes from the dev branch into the qa branch
7. Merge the changes from the qa branch into the prd branch
8. Review branch rules for all 3 branches to ensure they align with your desired processes/requirements
9. Review CODEOWNERS files on all 3 branches, add relevant groups/individuals to align with who you wish to define as code owners for a given branch (see the README in the docs folder for details)
10. Begin pushing code!
11. Once you have one or more Python scripts committed to the repo, set up and enable CodeQL code scanning. This is found under Settings > Code security and analysis within the "Code scanning section". Select **Set up** and click **Default**. You can find a walkthrough of these steps and more information about CodeQL here: https://docs.github.com/en/code-security/code-scanning/enabling-code-scanning/configuring-default-setup-for-code-scanning
        
(this is the end of the section to delete once setup is complete)

# Repo Overview

This repository contains the code for ingestion and integration of data for <INSERT LoB(S) HERE>. Each major folder contains a README with additional context for its contents.

## Branching & Merging Strategy

![branching_image](assets/github_branch_strategy.png)

Key features of the strategy:
- Feature branches created from the prd branch
    - Allows the dev branch to be a bit "messy" without worry that the mess will make it to production
- Consider minimal restrictions for what is allowed to merge into dev
    - Facilitates efficiency as there is less need to restrict what occurs on the dev branch
- Developer ownership of code is key - if something does not work or breaks, the merge should be reverted
    - Continue development on the feature branch
- The qa branch serves as a true testing ground
    - Ensure all updates work together
    - Full code review for updates moving to production
    - Allows ongoing sync between qa and prd
- Consider periodic rebasing of the dev branch to ensure it does not get too out of sync

### Branch Naming Conventions

The following branch types are used:
- feature: new code addition
- fix: correct existing code
- hotfix: urgent correction, often going directly into qa for immediate merge to produciton
- refactor: restructure functional code to optimize
- test: experimentation, PoC, etc.

Branch naming structure uses this convention:
    branch-type/jira-ticket/short_description where ther short description does not exceed 4 words
For example:
    feature/TGCP-1234/new_workflow

### Commit Message Conventions

Commits and the associated commit message create the history of the repo. It is important to commit regularly, ideally whenever a small section of work has been completed, in order to make this history meaningful for those reviewing commit history or auditing the work completed in the repo.

Commit messages have 4 major components:
1. Category
    - feature: new code addition
    - fix: correct existing code
    - hotfix: urgent correction, often going directly into qa for immediate merge to produciton
    - refactor: restructure functional code to optimize
    - test: experimentation, PoC, etc.
2. Scope
    -<These need to be defined in alignment with meaningful segments of data for the LoB(s) the repo supports>
    - repo: any work done for the functionality of the repo such as to documentation, code owners files, workflows, etc.
3. Jira ticket number
4. Brief description of the work included in the commit

For example:
    “feature(repo): TDGCP-2345; add README, first section complete”
Note the message starts with the type, followed by the scope in parentheses, then a colon, then the Jira ticket, then a semi-colon, and finally the brief description. If a commit includes work done on mulitple files, multiple short descriptions may be added, separated by semi-colons.

### PR Message Conventions

When a Pull Request is initiated, the message should include a brief summary of all the commits included in the PR. This summary should be concise, ideally using bullets to lay out distinct chunks of work. 

The reviewer should also include a meaningful message alongside their response. If changes are requested, [additional functionality](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/reviewing-changes-in-pull-requests/commenting-on-a-pull-request) is available to highlight the code in need of edits or propose such changes.

PRs should be treated as conversations, and making use of the commenting features helps to facilitate this.

## Releases

The release schedule for this repo is as follows:

- The qa branch is locked for further changes on <day/date> at <time>
- Code review takes place from <day/time> to <day/time>
- The qa branch is merged into prd on <day/date> at <time>
- [Semantic Versioning](https://semver.org/#semantic-versioning-specification-semver) conventions are utilized to designate release versions

## Repository & Code Documentation

- READMEs should be updated in conjunction with any major additions/updates to the primary folders of the repo
    - Standard sections: Overview, How it Works, Uses
- Comments should be used thoughtfully and regularly throughout code to allow others to more readily orient themselves to the contents of script
