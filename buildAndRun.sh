#!/bin/sh
mvn clean package && docker build -t muei.riws.tropes/tropes .
docker rm -f tropes || true && docker run -d -p 8080:8080 -p 4848:4848 --name tropes muei.riws.tropes/tropes 
