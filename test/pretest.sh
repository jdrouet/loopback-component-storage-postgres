#!/bin/bash

if [ -z "$PG_PORT" ]; then
  export PG_PORT=5432
fi

if [ -z "$PG_HOST" ]; then
  export PG_HOST=localhost
fi

export PG_USER=postgres

psql -p $PG_PORT -h $PG_HOST -U postgres <<- EOSQL
  CREATE USER "test-user";
  ALTER USER "test-user" WITH ENCRYPTED PASSWORD 'test-password';
  ALTER USER "test-user" WITH SUPERUSER;
  DROP DATABASE test;
  CREATE DATABASE test OWNER "test-user";
EOSQL

psql -p $PG_PORT -h $PG_HOST -U "test-user" test <<- EOSQL
  CREATE TABLE files (
    id SERIAL PRIMARY KEY,
    container TEXT NOT NULL,
    filename TEXT NOT NULL,
    mimetype TEXT,
    objectid INTEGER UNIQUE NOT NULL
  );
EOSQL
