#!/usr/bin/env bash

HADES_REPO_DIR="$HOME/development/other-repos/gandras/hades"
HADES_WD="$HOME/hades_working_dir"

cd $HADES_REPO_DIR
pipenv install
#pipenv shell
#https://stackoverflow.com/a/57941049/1106893
source "$(pipenv --venv)/bin/activate"


# pipenv from here
cd $HADES_WD
set -x
python3 -u $HADES_REPO_DIR/cli.py -d run-script -s netty4