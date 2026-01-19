# Code Owners for this Repo
Separate individuals and/or teams can be set up as code owners, and branch rules can then be set up to require approval from a code owner prior to pull requests being merged into the the associated long-lived branch. It is also possible to set more granular rules such that specific individuals/teams are required to review code that is stored in certain folders or of a certain file type. [Reference the GitHub docs](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/about-code-owners) to learn more about code owners and the CODEOWNERS file syntax.

General recommendations for use of code owners:
- Our recommendation is to set up a team of code owners and require code owner approval for Pull Requests into the prod branch via branch rules
- You may additionally (or instead) wish to use branch rules to limit who can complete merges into the prod branch
