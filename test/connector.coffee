expect = require('chai').expect
fs = require 'fs'
loopback = require 'loopback'
path = require 'path'
StorageService = require '../source'
request = require 'supertest'
LargeObjectManager = require('pg-large-object').LargeObjectManager

insertTestFile = (ds, done) ->
  ds.connector.db.query 'BEGIN TRANSACTION', ->
    man = new LargeObjectManager ds.connector.db
    man.createAndWritableStream 16384, (err, oid, stream) ->
      return done err if err
      stream.on 'finish', ->
        ds.connector.db.query 'INSERT INTO files (container, filename, mimetype, objectid) values ($1, $2, $3, $4)', ['my-cats', 'item.png', 'png', oid], (err, res) ->
          ds.connector.db.query 'COMMIT TRANSACTION', ->
            done err
      stream.on 'error', (err) ->
        ds.connector.db.query 'ROLLBACK TRANSACTION', ->
          done err
      read = fs.createReadStream path.join __dirname, 'files', 'item.png'
      read.pipe stream

config =
  connector: StorageService
  hostname: process.env.PG_HOST or '127.0.0.1'
  port: process.env.PG_PORT or 5432
  database: 'test'
  username: 'test-user'
  password: 'test-password'

describe 'postgres connector', ->

  agent       = null
  app         = null
  datasource  = null
  server      = null

  describe 'datasource', ->

    it 'should exist', ->
      expect(StorageService).to.exist

    describe 'default configuration', ->

      before (done) ->
        datasource = loopback.createDataSource config
        setTimeout done, 200

      it 'should create the datasource', ->
        expect(datasource).to.exist
        expect(datasource.connector).to.exist
        expect(datasource.settings).to.exist

      it 'should expose pg instance', ->
        expect(datasource.connector.pg).to.exist

      it 'should create the url', ->
        expect(datasource.settings.url).to.exist
        expect(datasource.settings.url).to.eql "postgres://#{config.username}:#{config.password}@#{config.hostname}:#{config.port}/#{config.database}"

      it 'should be connected', ->
        expect(datasource.connected).to.eql true

  describe 'model usage', ->

    model = null

    before (done) ->
      datasource = loopback.createDataSource config
      model = datasource.createModel 'MyModel'
      setTimeout done, 200

    it 'should create the model', ->
      expect(model).to.exist

    describe 'getContainers function', ->

      it 'should exist', ->
        expect(model.getContainers).to.exist

      it 'should return an empty list', (done) ->
        model.getContainers (err, list) ->
          expect(Array.isArray list).to.eql true
          done()

    describe 'getContainer function', ->

      it 'should exist', ->
        expect(model.getContainer).to.exist

    describe 'upload function', ->

      it 'should exist', ->
        expect(model.upload).to.exist

  describe 'application usage', ->

    app     = null
    ds      = null
    server  = null

    before (done) ->
      app = loopback()
      app.set 'port', 5000
      app.set 'url', '127.0.0.1'
      app.set 'legacyExplorer', false
      app.use loopback.rest()
      ds = loopback.createDataSource config
      model = ds.createModel 'MyModel', {},
        base: 'Model'
        plural: 'my-model'
      app.model model
      setTimeout done, 200

    beforeEach (done) ->
      ds.connector.db.query 'SELECT lo_unlink(l.oid) FROM pg_largeobject_metadata l', ->
        ds.connector.db.query 'DELETE FROM files WHERE true', done

    before (done) ->
      server = app.listen done

    after ->
      server.close()

    describe 'getContainers', ->

      describe 'without data', ->

        it 'should return an array', (done) ->
          request 'http://127.0.0.1:5000'
          .get '/my-model'
          .end (err, res) ->
            expect(res.status).to.equal 200
            expect(Array.isArray res.body).to.equal true
            expect(res.body.length).to.equal 0
            done()

      describe 'with data', ->

        beforeEach (done) ->
          insertTestFile ds, done

        it 'should return an array', (done) ->
          request 'http://127.0.0.1:5000'
          .get '/my-model'
          .end (err, res) ->
            expect(res.status).to.equal 200
            expect(Array.isArray res.body).to.equal true
            expect(res.body.length).to.equal 1
            expect(res.body[0].container).to.equal 'my-cats'
            done()

    describe 'getFiles', ->

      describe 'without data', ->

        it 'should return an array', (done) ->
          request 'http://127.0.0.1:5000'
          .get '/my-model/nothing'
          .end (err, res) ->
            expect(res.status).to.equal 200
            expect(res.body.container).to.equal 'nothing'
            expect(Array.isArray res.body.files).to.equal true
            expect(res.body.files.length).to.equal 0
            done()

      describe 'with data', ->

        beforeEach (done) ->
          insertTestFile ds, done

        it 'should return an array', (done) ->
          app.models.MyModel.getFiles 'my-cats', (err, res) ->
            expect(Array.isArray res.rows).to.equal true
            expect(res.rows.length).to.equal 1
            done()

        it 'should return an array', (done) ->
          request 'http://127.0.0.1:5000'
          .get '/my-model/my-cats'
          .end (err, res) ->
            expect(res.status).to.equal 200
            expect(res.body.container).to.equal 'my-cats'
            expect(Array.isArray res.body.files).to.equal true
            expect(res.body.files.length).to.equal 1
            done()

    describe 'destroyContainer', ->

      describe 'without data', ->

        it 'should return an array', (done) ->
          request 'http://127.0.0.1:5000'
          .delete '/my-model/no-container'
          .end (err, res) ->
            expect(res.status).to.equal 200
            done()

      describe 'with data', ->

        beforeEach (done) ->
          insertTestFile ds, done

        it 'should return an array', (done) ->
          request 'http://127.0.0.1:5000'
          .delete '/my-model/my-cats'
          .end (err, res) ->
            expect(res.status).to.equal 200
            done()

    describe 'upload', ->

      it 'should return 20x', (done) ->
        request 'http://127.0.0.1:5000'
        .post '/my-model/my-cats/upload'
        .attach 'file', path.join(__dirname, 'files', 'item.png')
        .end (err, res) ->
          expect(res.status).to.equal 200
          done()

    describe 'download', ->

      beforeEach (done) ->
        insertTestFile ds, done

      it 'should return the file', (done) ->
        request 'http://127.0.0.1:5000'
        .get '/my-model/my-cats/download/item.png'
        .end (err, res) ->
          expect(res.status).to.equal 200
          done()

    describe 'removeFile', ->

      beforeEach (done) ->
        insertTestFile ds, done

      it 'should drop the file', (done) ->
        request 'http://127.0.0.1:5000'
        .delete '/my-model/my-cats/files/item.png'
        .end (err, res) ->
          expect(res.status).to.equal 200
          done()
