#!/bin/bash

DEST="${1:-$PWD/hsi_xfer_env}"

conda create -y -p ${DEST} python=3.12
source activate ${DEST}
conda activate ${DEST}
pip3 install -r requirements.txt
python3 ./setup.py install --install-scripts ${DEST}/bin/hsi_xfer
