#! /bin/sh
docker rmi kafka/jmx
docker build -t kafka/jmx .