#!/bin/bash
echo "Ensuring current user is in the Docker group"
usermod -aG docker ${USER}
su -s ${USER}
chown "$USER":"$USER" /home/"$USER"/.docker -R
chmod g+rwx "$HOME/.docker" -R
cd ./docker
docker-compose up
