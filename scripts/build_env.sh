#!/bin/bash

source activate py3env
if [ $? -eq 0 ];
then
    pip uninstall -y -r ~/requirements.txt
    conda deactivate
    conda remove --name py3env --all -y
    conda create -n py3env -y -q --copy python=3.6 pip
    source activate py3env
    pip install --upgrade pip
    pip install -r ~/requirements.txt
    conda deactivate
else
    conda create -n py3env -y -q --copy python=3.6 pip
    source activate py3env
    pip install --upgrade pip
    pip install -r ~/requirements.txt
    conda deactivate
fi

echo "Zipping Environment......"
cd ~/.conda/envs
zip -q -r ../../py3env.zip py3env
cd ~
