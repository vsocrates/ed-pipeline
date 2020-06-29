# CDSW Template

A template for CDSW projects.

# How To Use

1. Fork the repository for your use case and name appropriately
2. If you haven't already done so, create a Personal Access Token and note it
3. Create a new CDSW project by selecting the git tab and input the URL of the forked repo, adding in `<user>`:`<token>`@ between https:// and cchteam.med
4. In the project settings, add two environment variables GIT_USER and GIT_TOKEN with their corresponding values.
5. Add any libraries installable with conda to environment.yml and any additional pip libraries to requirements.txt
6. Run build.py to create the conda environment. You can configure your jobs to depend on a build job or manually run build.py anytime your requirements change.
7. Create python code modules under modules.
8. Import your module into driver.py below the spark getOrCreate
9. Run driver.py manually or in a job
10. Do not increase the values in spark-defaults.conf
