#To the streaming job. run the below command
#sh run.sh
sh project_packages.sh
pip3 install -r requirements.txt
nohup python3 driver.py > output.log 2>&1 &