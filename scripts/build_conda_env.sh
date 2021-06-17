#!/bin/bash
export PIP_SRC="~/.conda/envs/py3env/"
source activate py3env
if [ $? -eq 0 ];
then
    conda env update --file ~/environment.yml --prune
else
    conda env create -f ~/environment.yml
fi

echo "Zipping Environment......"
cd ~/.conda/envs
zip -q -r ../../py3env.zip py3env
cd ~

conda deactivate
pip3 install -r requirements.txt
