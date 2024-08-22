#!/bin/zsh

DEV_DEST=${PWD}/hsi-xfer-dev-env
PROD_DEST=/sw/sources/hpss/hsi-xfer

if [[ "${1:-x}" == "prod" ]] ; then
	conda create -y -p ${PROD_DEST} python=3.9
	source activate ${PROD_DEST}
	conda activate ${PROD_DEST}
	pip3 install -r requirements.txt
	python3 ./setup.py install --install-scripts ${PROD_DEST}/bin/hsi_xfer
else
	rm -rf ${DEV_DEST}
	conda create -y -p ${DEV_DEST} python=3.12
	source activate ${DEV_DEST}
	conda activate ${DEV_DEST}
	pip3 install -r requirements.txt
	python3 ./setup.py install --install-scripts ${DEV_DEST}/bin/hsi_xfer
fi
