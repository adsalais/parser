#! /bin/bash
set -e

echo "/!Warning!\\"
echo "/!Warning!\ This will DELETE every docker containers and image on the system"
echo "/!Warning!\\"
read -p  "Do you want to proceed [Y/n] " -n 1 -r
echo    
if [[ $REPLY =~ ^[Yy]$ ]]
then
    # Stopping Instance
    echo "Stopping every docker containers"
    for i in $(docker ps | sed 's/ / /' | awk '{print $1}' | tail -n +2); do
        echo "Stopping $i"
        docker stop $i
    done

    # Removing Instance
    echo "Removing every docker containers"
    for i in $(docker ps -a | sed 's/ / /' | awk '{print $1}' | tail -n +2); do
      echo "Removing: $i"
      docker rm $i
    done 

    # Removing Images
    echo "Removing every docker images"
    for i in $(docker image ls | sed 's/ / /' | awk '{print $1}' | tail -n +2); do
      echo "Removing: $i"
      docker image rm $i
    done 
fi


cd jupyter
sh build.sh
cd -


