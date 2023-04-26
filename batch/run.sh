#!/bin/sh
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_KEY=adminadmin
/spark/bin/spark-submit --driver-memory 5G /batch/batch.py