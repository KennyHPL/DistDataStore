#!/bin/bash
#build run will build off the Dockerfile in the current directory and then run it

#build rm will kill all running containers and removes them, it also removes
#dangling images

if [[ $1 = "" ]]; then
  #build the container
  docker build --rm -t ptest .

elif [[ $1 = "main" ]]; then
  #run the built container in on port localhost:8080
  docker run -d --ip=10.0.0.20 -p 8083:8080 \
  --net=mynet \
  -e IP=10.0.0.20 \
  -e PORT=8080 \
  ptest

elif [[ $1 = "sec" ]]; then
    #run the built container in on port localhost:8080
    docker run -d -p 8084:8080 \
    --net=mynet \
    -e IP=10.0.0.21 \
    -e PORT=8080 \
    -e MAINIP=10.0.0.20:8080 \
    ptest

elif [[ $1 = "rm" ]]; then
  #kill all running containers
  docker kill $(docker ps -a -q)
  #remove all containers
  docker rm $(docker ps -a -q)
  #remove all dangling images
  docker rmi $(docker images -f "dangling=true" -q)

else
  echo Usage: ./build.sh main or ./build.sh sec or ./build.sh rm
fi
