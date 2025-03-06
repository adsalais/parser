#! /bin/sh
docker rmi jupyter/local
docker build -t jupyter/local .