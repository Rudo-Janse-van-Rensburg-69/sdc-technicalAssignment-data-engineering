#!/bin/bash
# Ensuring current user is in the Docker group
usermod -aG docker ${USER}
su -s ${USER}
chown "$USER":"$USER" /home/"$USER"/.docker -R
chmod g+rwx "$HOME/.docker" -R

# create docker container
echo "CREATING DATABASE..."
cd ./docker
docker-compose up &
cd ../code
echo "CREATING CODE ENVIRONMENT..."
# Set up python virtual environment
#python -m venv env
source ./bin/activate
./bin/pip install -r requirements.txt
echo "RUNNING CODE..."
for ((; ;))
do 
    ./bin/python main.py 
    sleep 7d
done
