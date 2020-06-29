import os

os.system("rm ./*.zip")
os.chmod("scripts/build_conda_env.sh", 0o755)
os.system("scripts/build_conda_env.sh")

