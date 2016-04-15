# loopback-component-storage-postgres
[![Build Status](https://travis-ci.org/jdrouet/loopback-component-storage-postgres.svg)](https://travis-ci.org/jdrouet/loopback-component-storage-postgres)
[![Dependency Status](https://david-dm.org/jdrouet/loopback-component-storage-postgres.svg)](https://david-dm.org/jdrouet/loopback-component-storage-postgres)
[![codecov.io](https://codecov.io/github/jdrouet/loopback-component-storage-postgres/coverage.svg?branch=master)](https://codecov.io/github/jdrouet/loopback-component-storage-postgres?branch=master)

LoopBack storage postgres component provides Node.js and REST APIs to manage binary contents using Postgres Large Object

## Installation

Install the storage component as usual for a Node package:

```bash
  npm install --save loopback-component-storage-postgres
```



## Using it

Create a table to store file descriptions

```sql
  CREATE TABLE "my-table-to-store-files" (
    id SERIAL PRIMARY KEY,
    container TEXT NOT NULL,
    filename TEXT NOT NULL,
    mimetype TEXT,
    objectid INTEGER UNIQUE NOT NULL
  );
```

Edit you datasources.json and add the following part

```javascript
"pg_file": {
  "name": "pg_file",
  "connector": "loopback-component-storage-postgres",
  "host": "localhost",
  "port": 5432,
  "database": "test",
  "table": "my-table-to-store-files",
  "username": "test-user",
  "password": "test-password"
}
```

And the you can use it as a datasource of your model.

## API

Description                                                   | Container model method                    | REST URI
--------------------------------------------------------------|-------------------------------------------|--------------------------------------------
List all containers                                           | getContainers(callback)                   | GET /api/\<model>
Get information about specified container                     | getContainer(container, callback)         | GET /api/\<model>/:container
Create a new container                                        | createContainer(options, callback)        | PORT /api/\<model>
Delete specified container                                    | destroyContainer(options, callback)       | DELETE /api/\<model>/:container
List all files within specified container                     | getFiles(container, callback)             | GET /api/\<model>/:container/files
Get information for specified file within specified container | getFile(container, file, callback)        | GET /api/\<model>/:container/files/:file
Delete a file within a given container by name                | removeFile(container, file, callback)     | DELETE /api/\<model>/:container/files/:file
Upload one or more files into the specified container         | upload(container, req, res, callback)     | POST /api/\<model>/:container/upload
Download a file within specified container                    | download(container, file, res, callback)  | GET /api/\<model>/:container/download/:file
