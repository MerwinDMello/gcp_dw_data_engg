# Environment Configuration Files

These files set up variables that can be used by other scripts in the repo. The majority of these variables are environment specific (dev, qa, prod), so a configuration file is created for each environment to align with this. All files should be present on each of the long-lived branches of the repo, and any updates must be promoted accordingly to avoid merge conflicts and unexpected behavior.

## Reminder
When updates/additions are needed for this file, the suggested process is to create a feature branch to be used exclusively for such updates/additions that can be quickly promoted up through all long-lived branches. **Do not** make changes to this file on a feature branch alongside other work that will not be able to be quickly promoted up through all environments or merge conflicts are likely to occur.