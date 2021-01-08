# CDSW Template

A template for CDSW projects.

# How To Use

1. If you haven't already done so, create a Personal Access Token and note it
2. Clone this repository to a local machine (make sure check-out-as-is, commit-unix-style line endings option enabled for your git install)
3. Create a new folder and copy all but the .git file from the cloned repo into the new folder
4. Create a new empty repo in Gitlab
5. Use the git init workflow to push the new folder up to the new gitlab repo
6. Create a new CDSW project by selecting the git tab and input the URL of the new repo, adding in `<user>`:`<token>`@ between https:// and cchteam.med
7. In the project settings, add two environment variables GIT_USER and GIT_TOKEN with their corresponding values.
8. Add any libraries installable with conda to environment.yml and any additional pip libraries to requirements.txt
9. Run build.py to create the conda environment. You can configure your jobs to depend on a build job or manually run build.py anytime your requirements change.
10. Create python code modules under modules.
11. Import your module into driver.py below the spark getOrCreate
12. Run driver.py manually or in a job
13. Do not increase the values in spark-defaults.conf
