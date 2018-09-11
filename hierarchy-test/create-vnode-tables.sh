#!/bin/bash

echo "BEGIN;" >> ./create-tables.sql;
echo "" >> ./create-tables.sql;

arr=("$@")
for i in "${arr[@]}";
do
    echo "CREATE TABLE IF NOT EXISTS manta_bucket_object_$i (" >> ./create-tables.sql
    echo "    id uuid NOT NULL," >> ./create-tables.sql
    echo "    name text NOT NULL," >> ./create-tables.sql
    echo "    owner uuid NOT NULL," >> ./create-tables.sql
    echo "    bucket_id uuid NOT NULL," >> ./create-tables.sql
    echo "    created timestamptz DEFAULT current_timestamp NOT NULL," >> ./create-tables.sql
    echo "    modified timestamptz DEFAULT current_timestamp NOT NULL," >> ./create-tables.sql
    echo "    vnode bigint NOT NULL," >> ./create-tables.sql
    echo "    creator uuid," >> ./create-tables.sql
    echo "    content_length bigint," >> ./create-tables.sql
    echo "    content_md5 text," >> ./create-tables.sql
    echo "    content_type text," >> ./create-tables.sql
    echo "    headers hstore," >> ./create-tables.sql
    echo "    sharks hstore," >> ./create-tables.sql
    echo "    properties jsonb," >> ./create-tables.sql
    echo "    PRIMARY KEY (owner, bucket_id, name)" >> ./create-tables.sql
    echo ");" >> ./create-tables.sql
    echo "" >> ./create-tables.sql;
    echo "" >> ./create-tables.sql;
done

echo "COMMIT;" >> ./create-tables.sql;
