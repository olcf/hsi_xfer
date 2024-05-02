SHELL:=/bin/bash
GROUPOWN:=ccsstaff
OWNER:=wyn
DEST:=/sw/sources/hpss/bin
SCRIPTPERMS:=777
DIRPERMS:=777

all: dev prod

prep:
	python3 -m venv venv
	source venv/bin/activate
	${PWD}/venv/bin/pip install -r requirements.txt

dev: prep
	${PWD}/venv/bin/pex -r requirements.txt -o hpss_transfer_sorted_files_dev --console-script ${PWD}/hpss_transfer_sorted_files.py

prod: prep
	${PWD}/venv/bin/pex -r requirements.txt -o hpss_transfer_sorted_files --console-script ${DEST}/hpss_transfer_sorted_files/hpss_transfer_sorted_files.py

requirements:
	pip freeze > requirements.txt

install: all
	# create and set perms for the script directory
	mkdir -p ${DEST}/hpss_transfer_sorted_files
	chown ${OWNER}:${GROUPOWN} ${DEST}/hpss_transfer_sorted_files
	chmod ${DIRPERMS} ${DEST}/hpss_transfer_sorted_files
	# Copy and set perms for the script
	cp hpss_transfer_sorted_files.py ${DEST}/hpss_transfer_sorted_files/
	chown ${OWNER}:${GROUPOWN} ${DEST}/hpss_transfer_sorted_files/hpss_transfer_sorted_files.py
	chmod ${SCRIPTPERMS} ${DEST}/hpss_transfer_sorted_files/hpss_transfer_sorted_files.py
	# Copy and set perms for the binary
	cp hpss_transfer_sorted_files ${DEST}/
	chown ${OWNER}:${GROUPOWN} ${DEST}/hpss_transfer_sorted_files
	chmod ${SCRIPTPERMS} ${DEST}/hpss_transfer_sorted_files
