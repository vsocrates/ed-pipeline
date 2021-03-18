# CDSW Template

Template for CDSW Projects

1.  Create access token in gitlab
2.  Create new empty repo in gitlab
3.  Create CDSW project using git as starting option (or clone to local)
4.  git url is https://\<user>:\<token>@cchteam.med.yale.edu/gitlab/computationalhealth/datasci/cdsw-template.git
5.  Start session -> Terminal Access
6.  git remote set-url origin \<yournewprojectrepo\>
7.  git push --all
8.  Add environment variables GIT_USER and GIT_TOKEN to CDSW project
9.  Add any necessary libraries to both environment.yml (if conda repo for it exists) as well as requirements.txt
10.  Run build.py
11.  Develop
