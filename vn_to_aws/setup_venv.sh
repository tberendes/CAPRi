#!/bin/bash
#mkdir folder
#cd folder
virtualenv v-env
source ./v-env/bin/activate
pip3 install -r requirements.txt
deactivate
