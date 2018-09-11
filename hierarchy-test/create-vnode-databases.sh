#!/bin/bash

arr=("$@")
for i in "${arr[@]}";
do
    echo "CREATE DATABASE manta_bucket_database_$i;" >> ./create-databases.sql;
    echo "CREATE TABLE IF NOT EXISTS manta_bucket_database_$i.public.manta_bucket_object (" >> ./create-databases.sql
    echo "    id uuid NOT NULL," >> ./create-databases.sql
    echo "    name text NOT NULL," >> ./create-databases.sql
    echo "    owner uuid NOT NULL," >> ./create-databases.sql
    echo "    bucket_id uuid NOT NULL," >> ./create-databases.sql
    echo "    created timestamptz DEFAULT current_timestamp NOT NULL," >> ./create-databases.sql
    echo "    modified timestamptz DEFAULT current_timestamp NOT NULL," >> ./create-databases.sql
    echo "    vnode bigint NOT NULL," >> ./create-databases.sql
    echo "    creator uuid," >> ./create-databases.sql
    echo "    content_length bigint," >> ./create-databases.sql
    echo "    content_md5 text," >> ./create-databases.sql
    echo "    content_type text," >> ./create-databases.sql
    echo "    headers hstore," >> ./create-databases.sql
    echo "    sharks hstore," >> ./create-databases.sql
    echo "    properties jsonb," >> ./create-databases.sql
    echo "    PRIMARY KEY (owner, bucket_id, name)" >> ./create-databases.sql
    echo ");" >> ./create-databases.sql
    echo "" >> ./create-databases.sql;
    echo "" >> ./create-databases.sql;
done
