#!/bin/bash
# Start ssh server
service ssh restart 

# Starting the services
bash start-services.sh

# Creating a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install any packages
pip install -r requirements.txt  

# Package the virtual env.
venv-pack -o .venv.tar.gz

# Collect data
bash prepare_data.sh


bash index.sh /index/data

bash search.sh "Film"
bash search.sh "Food"
bash search.sh "Baby"
#tail -f /dev/null
