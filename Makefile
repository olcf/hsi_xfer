SHELL:=/bin/bash
GROUPOWN:=ccsstaff
OWNER:=wyn
DEST:=/sw/sources/hpss/bin
SCRIPTPERMS:=777
DIRPERMS:=777
VENV:=venv-tmp
DEV_EXE:=hpss_transfer_sorted_files_dev
PROD_EXE:=hpss_transfer_sorted_files

all: dev prod remove_venv

clean: remove_venv
	rm -f ${PWD}/${DEV_EXE}
	rm -f ${PWD}/${PROD_EXE}

prep:
	python3 -m venv ${VENV}
	source ${PWD}/${VENV}/bin/activate
	${PWD}/${VENV}/bin/pip install -r requirements.txt

dev: prep
	${PWD}/venv/bin/pex -r requirements.txt -o hpss_transfer_sorted_files_dev --console-script ${PWD}/hpss_transfer_sorted_files.py

prod: prep
	${PWD}/venv/bin/pex -r requirements.txt -o hpss_transfer_sorted_files --console-script ${DEST}/.hpss_transfer_sorted_files_script/hpss_transfer_sorted_files.py

remove_venv:
	#deactivate
	rm -rf ./${VENV}

requirements:
	pip freeze > requirements.txt

script_install:
	mkdir -p ${DEST}/.hpss_transfer_sorted_files_script
	# Copy the script
	cp hpss_transfer_sorted_files.py ${DEST}/.hpss_transfer_sorted_files_script/

install: script_install all
	# Copy the binary
	cp hpss_transfer_sorted_files ${DEST}/
