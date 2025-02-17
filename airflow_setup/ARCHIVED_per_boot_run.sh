#!/bin/bash

# Copy automate_airflow.sh contents to the per-boot directory so that this script runs every time we boot up the ec2
sudo sh -c 'cat automate_airflow.sh > /var/lib/cloud/scripts/per-boot/automated-script.sh'

# Make the script executable
sudo chmod +x /var/lib/cloud/scripts/per-boot/automated-script.sh
