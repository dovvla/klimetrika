#!/bin/sh
docker compose up -d; docker exec spark-master sh fixes.sh;
