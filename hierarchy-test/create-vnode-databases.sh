#!/bin/bash

arr=("$@")
for i in "${arr[@]}";
do
    db_filename="create-databases-$i.sql";
    table_filename="create-databases-table-$i.sql";
    echo "CREATE DATABASE manta_bucket_$i;" >> $db_filename;
    echo "" >> $db_filename;
    echo "CREATE EXTENSION hstore;" >> $table_filename
    echo "CREATE EXTENSION pgcrypto;" >> $table_filename
    echo "CREATE TABLE IF NOT EXISTS manta_bucket_$i.public.manta_bucket_object (" >> $table_filename
    echo "    id uuid NOT NULL," >> $table_filename
    echo "    name text NOT NULL," >> $table_filename
    echo "    owner uuid NOT NULL," >> $table_filename
    echo "    bucket_id uuid NOT NULL," >> $table_filename
    echo "    created timestamptz DEFAULT current_timestamp NOT NULL," >> $table_filename
    echo "    modified timestamptz DEFAULT current_timestamp NOT NULL," >> $table_filename
    echo "    vnode bigint NOT NULL," >> $table_filename
    echo "    creator uuid," >> $table_filename
    echo "    content_length bigint," >> $table_filename
    echo "    content_md5 text," >> $table_filename
    echo "    content_type text," >> $table_filename
    echo "    headers hstore," >> $table_filename
    echo "    sharks hstore," >> $table_filename
    echo "    properties jsonb," >> $table_filename
    echo "    PRIMARY KEY (owner, bucket_id, name)" >> $table_filename
    echo ");" >> $table_filename
    echo "" >> $table_filename;
    echo "" >> $table_filename;
done
