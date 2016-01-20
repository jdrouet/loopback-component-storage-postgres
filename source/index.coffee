_ = require 'lodash'
async = require 'async'
Busboy = require 'busboy'
DataSource = require('loopback-datasource-juggler').DataSource
debug = require('debug') 'loopback:storage:postgres'
pg = require 'pg'
LargeObjectManager = require('pg-large-object').LargeObjectManager
Promise = require 'bluebird'

generateUrl = (options) ->
  host      = options.host or options.hostname or 'localhost'
  port      = options.port or 5432
  database  = options.database or 'test'
  if options.username and options.password
    return "postgres://#{options.username}:#{options.password}@#{host}:#{port}/#{database}"
  else
    return "postgres://#{host}:#{port}/#{database}"

class PostgresStorage
  constructor: (@settings) ->
    if not @settings.table
      @settings.table = 'files'
    if not @settings.url
      @settings.url = generateUrl @settings

  connect: (callback) ->
    self = @
    if @db
      process.nextTick ->
        if callback
          callback null, self.db
    else
      self.db = new pg.Client self.settings.url
      self.db.connect (err) ->
        debug 'Postgres connection established: ' + self.settings.url
        return callback err, self.db if callback

  getContainers: (callback) ->
    @db.query "select distinct container from #{@settings.table}", [], (err, res) ->
      callback err, res?.rows

  getContainer: (name, callback) ->
    @db.query "select * from #{@settings.table} where container = $1", [name], (err, res) ->
      callback err,
        container: name
        files: res?.rows

  destroyContainer: (name, callback) ->
    self = @
    self.db.query 'BEGIN TRANSACTION', (err) ->
      return callback err if err
      async.waterfall [
        (done) ->
          sql = "select lo_unlink(objectid) from #{@settings.table} where container = $1"
          self.db.query sql, [name], (err) ->
            done err
        (done) ->
          sql = "delete from #{self.settings.table} where container = $1"
          self.db.query sql, [name], done
      ], (err, res) ->
        if err
          self.db.query 'ROLLBACK TRANSACTION', ->
            callback err
        else
          self.db.query 'COMMIT TRANSACTION', (err) ->
            return callback err, res

  upload: (container, req, res, callback) ->
    self = @
    busboy = new Busboy headers: req.headers
    promises = []
    busboy.on 'file', (fieldname, file, filename, encoding, mimetype) ->
      promises.push new Promise (resolve, reject) ->
        options =
          container: container
          filename: filename
          mimetype: mimetype
        self.uploadFile container, file, options, (err, res) ->
          return reject err if err
          resolve res
    busboy.on 'finish', ->
      Promise.all promises
      .then (res) ->
        return callback null, res
      .catch callback
    req.pipe busboy

  uploadFile: (container, file, options, callback = (-> return)) ->
    self = @
    handleError = (err) ->
      self.db.query 'ROLLBACK TRANSACTION', ->
        callback err

    self.db.query 'BEGIN TRANSACTION', (err) ->
      return callback err if err
      # TODO parametrize bufferSize
      bufferSize = 16384
      man = new LargeObjectManager self.db
      man.createAndWritableStream bufferSize, (err, objectid, stream) ->
        return handleError err if err
        stream.on 'finish', ->
          self.db.query "insert into #{self.settings.table} (container, filename, mimetype, objectid) values ($1, $2, $3, $4) RETURNING *"
          , [options.container, options.filename, options.mimetype, objectid]
          , (err, res) ->
            return handleError err if err
            self.db.query 'COMMIT TRANSACTION', (err) ->
              return handleError err if err
              callback null, res.rows[0]
        stream.on 'error', handleError
        file.pipe stream

  getFiles: (container, callback) ->
    @db.query "select * from #{@settings.table} where container = $1", [name], callback
  
  removeFile: (container, filename, callback) ->
    self = @
    self.db.query 'BEGIN TRANSACTION', (err) ->
      return callback err if err
      async.waterfall [
        (done) ->
          sql = "select lo_unlink(objectid) from #{self.settings.table} where container = $1 and filename = $2"
          self.db.query sql, [container, filename], (err) ->
            done err
        (done) ->
          sql = "delete from #{self.settings.table} where container = $1 and filename = $2"
          self.db.query sql, [container, filename], done
      ], (err, res) ->
        if err
          self.db.query 'ROLLBACK TRANSACTION', ->
            callback err
        else
          self.db.query 'COMMIT TRANSACTION', (err) ->
            return callback err, res

  getFile: (container, filename, callback) ->
    @db.query "select * from #{@settings.table} where container = $1 and filename = $2"
    , [container, filename]
    , (err, res) ->
      return callback err if err
      if not res or not res.rows or res.rows.length is 0
        err = new Error 'File not found'
        err.status = 404
        return callback err
      callback null, res.rows[0]

  download: (container, filename, res, callback = (-> return)) ->
    self = @
    self.db.query 'BEGIN TRANSACTION', (err) ->
      return callback err if err
      self.getFile container, filename, (err, file) ->
        return callback err if err
        # TODO parametrize bufferSize
        bufferSize = 16384
        man = new LargeObjectManager self.db
        man.openAndReadableStream file.objectid, bufferSize, (err, size, stream) ->
          return callback err if err
          res.set 'Content-Disposition', "attachment; filename=\"#{file.filename}\""
          res.set 'Content-Type', file.mimetype
          res.set 'Content-Length', size
          stream.pipe res

PostgresStorage.modelName = 'storage'

PostgresStorage.prototype.getContainers.shared = true
PostgresStorage.prototype.getContainers.accepts = []
PostgresStorage.prototype.getContainers.returns = {arg: 'containers', type: 'array', root: true}
PostgresStorage.prototype.getContainers.http = {verb: 'get', path: '/'}

PostgresStorage.prototype.getContainer.shared = true
PostgresStorage.prototype.getContainer.accepts = [{arg: 'container', type: 'string'}]
PostgresStorage.prototype.getContainer.returns = {arg: 'containers', type: 'object', root: true}
PostgresStorage.prototype.getContainer.http = {verb: 'get', path: '/:container'}

PostgresStorage.prototype.destroyContainer.shared = true
PostgresStorage.prototype.destroyContainer.accepts = [{arg: 'container', type: 'string'}]
PostgresStorage.prototype.destroyContainer.returns = {}
PostgresStorage.prototype.destroyContainer.http = {verb: 'delete', path: '/:container'}

PostgresStorage.prototype.upload.shared = true
PostgresStorage.prototype.upload.accepts = [
  {arg: 'container', type: 'string'}
  {arg: 'req', type: 'object', http: {source: 'req'}}
  {arg: 'res', type: 'object', http: {source: 'res'}}
]
PostgresStorage.prototype.upload.returns = {arg: 'result', type: 'object'}
PostgresStorage.prototype.upload.http = {verb: 'post', path: '/:container/upload'}

PostgresStorage.prototype.getFiles.shared = true
PostgresStorage.prototype.getFiles.accepts = [
  {arg: 'container', type: 'string'}
]
PostgresStorage.prototype.getFiles.returns = {arg: 'file', type: 'array', root: true}
PostgresStorage.prototype.getFiles.http = {verb: 'get', path: '/:container/files'}

PostgresStorage.prototype.getFile.shared = true
PostgresStorage.prototype.getFile.accepts = [
  {arg: 'container', type: 'string'}
  {arg: 'file', type: 'string'}
]
PostgresStorage.prototype.getFile.returns = {arg: 'file', type: 'object', root: true}
PostgresStorage.prototype.getFile.http = {verb: 'get', path: '/:container/files/:file'}

PostgresStorage.prototype.removeFile.shared = true
PostgresStorage.prototype.removeFile.accepts = [
  {arg: 'container', type: 'string'}
  {arg: 'file', type: 'string'}
]
PostgresStorage.prototype.removeFile.returns = {}
PostgresStorage.prototype.removeFile.http = {verb: 'delete', path: '/:container/files/:file'}

PostgresStorage.prototype.download.shared = true
PostgresStorage.prototype.download.accepts = [
  {arg: 'container', type: 'string'}
  {arg: 'file', type: 'string'}
  {arg: 'res', type: 'object', http: {source: 'res'}}
]
PostgresStorage.prototype.download.http = {verb: 'get', path: '/:container/download/:file'}

exports.initialize = (dataSource, callback) ->
  settings = dataSource.settings or {}
  connector = new PostgresStorage settings
  dataSource.connector = connector
  dataSource.connector.dataSource = dataSource
  connector.DataAccessObject = -> return
  for m, method of PostgresStorage.prototype
    if _.isFunction method
      connector.DataAccessObject[m] = method.bind connector
      for k, opt of method
        connector.DataAccessObject[m][k] = opt
  connector.define = (model, properties, settings) -> return
  if callback
    dataSource.connector.connect callback
  return
