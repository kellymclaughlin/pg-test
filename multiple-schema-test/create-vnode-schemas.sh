#!/bin/bash

echo "BEGIN;" >> ./create-schemas.sql;
echo "" >> ./create-schemas.sql;

arr=("$@")
for i in "${arr[@]}";
do
    echo "CREATE SCHEMA IF NOT EXISTS manta_bucket_$i;" >> ./create-schemas.sql;
    echo "CREATE TABLE IF NOT EXISTS manta_bucket_$i.manta_bucket_object (" >> ./create-schemas.sql
    echo "    id uuid NOT NULL," >> ./create-schemas.sql
    echo "    name text NOT NULL," >> ./create-schemas.sql
    echo "    owner uuid NOT NULL," >> ./create-schemas.sql
    echo "    bucket_id uuid NOT NULL," >> ./create-schemas.sql
    echo "    created timestamptz DEFAULT current_timestamp NOT NULL," >> ./create-schemas.sql
    echo "    modified timestamptz DEFAULT current_timestamp NOT NULL," >> ./create-schemas.sql
    echo "    vnode bigint NOT NULL," >> ./create-schemas.sql
    echo "    creator uuid," >> ./create-schemas.sql
    echo "    content_length bigint," >> ./create-schemas.sql
    echo "    content_md5 text," >> ./create-schemas.sql
    echo "    content_type text," >> ./create-schemas.sql
    echo "    headers hstore," >> ./create-schemas.sql
    echo "    sharks hstore," >> ./create-schemas.sql
    echo "    properties jsonb," >> ./create-schemas.sql
    echo "    PRIMARY KEY (owner, bucket_id, name)" >> ./create-schemas.sql
    echo ");" >> ./create-schemas.sql
    echo "" >> ./create-schemas.sql;
    echo "" >> ./create-schemas.sql;
done

echo "COMMIT;" >> ./create-schemas.sql;
