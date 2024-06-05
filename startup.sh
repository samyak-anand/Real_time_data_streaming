#!/bin/bash
set -e

sudo apt install -y acl

mkdir -p ./dags ./logs
chmod -R +x script

sudo chmod -R 777 dags
sudo chmod -R 777 logs
sudo setfacl -d -m u::rwx,g::rwx,o::rwx dags
sudo setfacl -d -m u::rwx,g::rwx,o::rwx logs

sudo docker compose up

