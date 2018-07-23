#!/bin/bash

echo "BEGIN;" >> ./deploy/appschema.sql;
echo "" >> ./deploy/appschema.sql;

arr=("$@")
for i in "${arr[@]}";
do
    echo "CREATE SCHEMA IF NOT EXISTS manta_bucket_$i;" >> ./deploy/appschema.sql;
    echo "CREATE TABLE IF NOT EXISTS manta_bucket_$i.manta_bucket (" >> ./deploy/appschema.sql
    echo "    key text NOT NULL," >> ./deploy/appschema.sql
    echo "    bucket text NOT NULL," >> ./deploy/appschema.sql
    echo "    mtime timestamptz DEFAULT current_timestamp NOT NULL," >> ./deploy/appschema.sql
    echo "    vnode bigint NOT NULL," >> ./deploy/appschema.sql
    echo "    owner uuid NOT NULL," >> ./deploy/appschema.sql
    echo "    creator uuid," >> ./deploy/appschema.sql
    echo "    \"objectId\" uuid NOT NULL," >> ./deploy/appschema.sql
    echo "    \"contentLength\" bigint," >> ./deploy/appschema.sql
    echo "    \"contentMD5\" text," >> ./deploy/appschema.sql
    echo "    \"contentType\" text," >> ./deploy/appschema.sql
    echo "    headers text[]," >> ./deploy/appschema.sql
    echo "    sharks text[]," >> ./deploy/appschema.sql
    echo "    PRIMARY KEY (owner, bucket, key)" >> ./deploy/appschema.sql
    echo ");" >> ./deploy/appschema.sql
    echo "" >> ./deploy/appschema.sql;
    echo "CREATE INDEX IF NOT EXISTS manta_bucket_owner_bucket_idx ON manta_bucket_$i.manta_bucket (owner, bucket);" >> ./deploy/appschema.sql
    echo "" >> ./deploy/appschema.sql;
    echo "" >> ./deploy/appschema.sql;
done

echo "COMMIT;" >> ./deploy/appschema.sql;
