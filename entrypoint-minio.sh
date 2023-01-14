#!/bin/sh
/usr/bin/mc config host add myminio http://minio:9000 admin adminadmin;
/usr/bin/mc rb -r --force myminio/raw;
/usr/bin/mc rb -r --force myminio/transform;
/usr/bin/mc rb -r --force myminio/curated;
/usr/bin/mc mb myminio/raw; 
/usr/bin/mc mb myminio/transform; 
/usr/bin/mc mb myminio/curated; 
/usr/bin/mc policy download myminio/raw; 
/usr/bin/mc cp -r /tmp/data myminio/raw;
exit 0; 