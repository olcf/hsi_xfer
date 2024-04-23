all:
	#python3 -m venv venv
	#source venv/bin/activate
	${PWD}/venv/bin/pip install -r requirements.txt
	${PWD}/venv/bin/pex -r requirements.txt -o hpss_transfer_sorted_files_dev --console-script ${PWD}/hpss_transfer_sorted_files.py
	${PWD}/venv/bin/pex -r requirements.txt -o hpss_transfer_sorted_files --console-script /sw/sources/hpss/bin/hpss_transfer_sorted_files.py
	#venv/bin/deactivate
