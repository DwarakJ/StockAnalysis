#!/bin/bash

virtualenv -p python3.8 ENV
./ENV/bin/pip3 install pip3 --upgrade
./ENV/bin/pip3 install --use-deprecated=legacy-resolver -r requirements.txt