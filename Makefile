.DEFAULT_GOAL := default
#################### PACKAGE ACTIONS ###################
reinstall_package:
	@pip uninstall -y chicago_crime|| :
	@pip install -e .pip install ipdb


train_cloud:
	@echo "Starting compute instance..."
	@gcloud compute instances start chicago-crimes-cpu --zone=europe-west1-c
	@echo "Copying files to VM..."
	@scp -r -i ~/.ssh/gcp-chicago-crimes chicago_crime/* jonah@34.22.230.224:~/chicago_crime/
	@scp -r -i ~/.ssh/gcp-chicago-crimes .env jonah@34.22.230.224:~/.env
	@echo "Starting training process on cloud VM..."
	@ssh -i ~/.ssh/gcp-chicago-crimes jonah@34.22.230.224 "export PYTHONPATH=/home/jonah && python ~/chicago_crime/interface/main.py && sudo shutdown now"
