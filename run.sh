#!/bin/bash
# Ensuring current user is in the Docker group
usermod -aG docker ${USER}
su -s ${USER}
chown "$USER":"$USER" /home/"$USER"/.docker -R
chmod g+rwx "$HOME/.docker" -R

# create docker container
cd ./docker
docker-compose up &
cd ../code
# Set up virtual environment
python -m venv env
source ./env/bin/activate
./bin/pip install -r requirements.txt 
./bin/python main.py 

