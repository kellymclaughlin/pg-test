#!/bin/bash

echo "BEGIN;" >> ./drop-schemas.sql;
echo "" >> ./drop-schemas.sql;

arr=("$@")
for i in "${arr[@]}";
do
    echo "DROP TABLE IF EXISTS manta_bucket_$i.manta_bucket_object;" >> ./drop-schemas.sql
    echo "DROP SCHEMA IF EXISTS manta_bucket_$i;" >> ./drop-schemas.sql;

    echo "" >> ./drop-schemas.sql;
done

echo "COMMIT;" >> ./drop-schemas.sql;
