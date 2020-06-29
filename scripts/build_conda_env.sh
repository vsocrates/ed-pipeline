#!/bin/bash
mkdir -p ~/.ssh
chmod 700 ~/.ssh
ssh-keygen -F github1vp.ynhh.org 2>/dev/null 1>/dev/null
if [ $? -eq 0 ];
then
  echo “github host is already known”
else
  ssh-keyscan -t rsa -T 10 github1vp.ynhh.org >> ~/.ssh/known_hosts
fi

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
