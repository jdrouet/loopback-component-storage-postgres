var Busboy, DataSource, LargeObjectManager, PostgresStorage, Promise, _, async, debug, generateUrl, pg;

_ = require('lodash');

async = require('async');

Busboy = require('busboy');

DataSource = require('loopback-datasource-juggler').DataSource;

debug = require('debug')('loopback:storage:postgres');

pg = require('pg');

LargeObjectManager = require('pg-large-object').LargeObjectManager;

Promise = require('bluebird');

generateUrl = function(options) {
  var database, host, port;
  host = options.host || options.hostname || 'localhost';
  port = options.port || 5432;
  database = options.database || 'test';
  if (options.username && options.password) {
    return "postgres://" + options.username + ":" + options.password + "@" + host + ":" + port + "/" + database;
  } else {
    return "postgres://" + host + ":" + port + "/" + database;
  }
};

PostgresStorage = (function() {
  function PostgresStorage(settings1) {
    this.settings = settings1;
    if (!this.settings.table) {
      this.settings.table = 'files';
    }
    if (!this.settings.url) {
      this.settings.url = generateUrl(this.settings);
    }
  }

  PostgresStorage.prototype.connect = function(callback) {
    var self;
    self = this;
    if (this.db) {
      return process.nextTick(function() {
        if (callback) {
          return callback(null, self.db);
        }
      });
    } else {
      self.db = new pg.Client(self.settings.url);
      return self.db.connect(function(err) {
        debug('Postgres connection established: ' + self.settings.url);
        if (callback) {
          return callback(err, self.db);
        }
      });
    }
  };

  PostgresStorage.prototype.getContainers = function(callback) {
    return this.db.query("select distinct container from " + this.settings.table, [], function(err, res) {
      return callback(err, res != null ? res.rows : void 0);
    });
  };

  PostgresStorage.prototype.getContainer = function(name, callback) {
    return this.db.query("select * from " + this.settings.table + " where container = $1", [name], function(err, res) {
      return callback(err, {
        container: name,
        files: res != null ? res.rows : void 0
      });
    });
  };

  PostgresStorage.prototype.destroyContainer = function(name, callback) {
    var self;
    self = this;
    return self.db.query('BEGIN TRANSACTION', function(err) {
      if (err) {
        return callback(err);
      }
      return async.waterfall([
        function(done) {
          var sql;
          sql = "select lo_unlink(objectid) from " + this.settings.table + " where container = $1";
          return self.db.query(sql, [name], function(err) {
            return done(err);
          });
        }, function(done) {
          var sql;
          sql = "delete from " + self.settings.table + " where container = $1";
          return self.db.query(sql, [name], done);
        }
      ], function(err, res) {
        if (err) {
          return self.db.query('ROLLBACK TRANSACTION', function() {
            return callback(err);
          });
        } else {
          return self.db.query('COMMIT TRANSACTION', function(err) {
            return callback(err, res);
          });
        }
      });
    });
  };

  PostgresStorage.prototype.upload = function(container, req, res, callback) {
    var busboy, promises, self;
    self = this;
    busboy = new Busboy({
      headers: req.headers
    });
    promises = [];
    busboy.on('file', function(fieldname, file, filename, encoding, mimetype) {
      return promises.push(new Promise(function(resolve, reject) {
        var options;
        options = {
          container: container,
          filename: filename,
          mimetype: mimetype
        };
        return self.uploadFile(container, file, options, function(err, res) {
          if (err) {
            return reject(err);
          }
          return resolve(res);
        });
      }));
    });
    busboy.on('finish', function() {
      return Promise.all(promises).then(function(res) {
        return callback(null, res);
      })["catch"](callback);
    });
    return req.pipe(busboy);
  };

  PostgresStorage.prototype.uploadFile = function(container, file, options, callback) {
    var handleError, self;
    if (callback == null) {
      callback = (function() {});
    }
    self = this;
    handleError = function(err) {
      return self.db.query('ROLLBACK TRANSACTION', function() {
        return callback(err);
      });
    };
    return self.db.query('BEGIN TRANSACTION', function(err) {
      var bufferSize, man;
      if (err) {
        return callback(err);
      }
      bufferSize = 16384;
      man = new LargeObjectManager(self.db);
      return man.createAndWritableStream(bufferSize, function(err, objectid, stream) {
        if (err) {
          return handleError(err);
        }
        stream.on('finish', function() {
          return self.db.query("insert into " + self.settings.table + " (container, filename, mimetype, objectid) values ($1, $2, $3, $4) RETURNING *", [options.container, options.filename, options.mimetype, objectid], function(err, res) {
            if (err) {
              return handleError(err);
            }
            return self.db.query('COMMIT TRANSACTION', function(err) {
              if (err) {
                return handleError(err);
              }
              return callback(null, res.rows[0]);
            });
          });
        });
        stream.on('error', handleError);
        return file.pipe(stream);
      });
    });
  };

  PostgresStorage.prototype.getFiles = function(container, callback) {
    return this.db.query("select * from " + this.settings.table + " where container = $1", [container], callback);
  };

  PostgresStorage.prototype.removeFile = function(container, filename, callback) {
    var self;
    self = this;
    return self.db.query('BEGIN TRANSACTION', function(err) {
      if (err) {
        return callback(err);
      }
      return async.waterfall([
        function(done) {
          var sql;
          sql = "select lo_unlink(objectid) from " + self.settings.table + " where container = $1 and filename = $2";
          return self.db.query(sql, [container, filename], function(err) {
            return done(err);
          });
        }, function(done) {
          var sql;
          sql = "delete from " + self.settings.table + " where container = $1 and filename = $2";
          return self.db.query(sql, [container, filename], done);
        }
      ], function(err, res) {
        if (err) {
          return self.db.query('ROLLBACK TRANSACTION', function() {
            return callback(err);
          });
        } else {
          return self.db.query('COMMIT TRANSACTION', function(err) {
            return callback(err, res);
          });
        }
      });
    });
  };

  PostgresStorage.prototype.getFile = function(container, filename, callback) {
    return this.db.query("select * from " + this.settings.table + " where container = $1 and filename = $2", [container, filename], function(err, res) {
      if (err) {
        return callback(err);
      }
      if (!res || !res.rows || res.rows.length === 0) {
        err = new Error('File not found');
        err.status = 404;
        return callback(err);
      }
      return callback(null, res.rows[0]);
    });
  };

  PostgresStorage.prototype.download = function(container, filename, res, callback) {
    var self;
    if (callback == null) {
      callback = (function() {});
    }
    self = this;
    return self.db.query('BEGIN TRANSACTION', function(err) {
      if (err) {
        return callback(err);
      }
      return self.getFile(container, filename, function(err, file) {
        var bufferSize, man;
        if (err) {
          return callback(err);
        }
        bufferSize = 16384;
        man = new LargeObjectManager(self.db);
        return man.openAndReadableStream(file.objectid, bufferSize, function(err, size, stream) {
          if (err) {
            return callback(err);
          }
          res.set('Content-Disposition', "attachment; filename=\"" + file.filename + "\"");
          res.set('Content-Type', file.mimetype);
          res.set('Content-Length', size);
          return stream.pipe(res);
        });
      });
    });
  };

  return PostgresStorage;

})();

PostgresStorage.modelName = 'storage';

PostgresStorage.prototype.getContainers.shared = true;

PostgresStorage.prototype.getContainers.accepts = [];

PostgresStorage.prototype.getContainers.returns = {
  arg: 'containers',
  type: 'array',
  root: true
};

PostgresStorage.prototype.getContainers.http = {
  verb: 'get',
  path: '/'
};

PostgresStorage.prototype.getContainer.shared = true;

PostgresStorage.prototype.getContainer.accepts = [
  {
    arg: 'container',
    type: 'string'
  }
];

PostgresStorage.prototype.getContainer.returns = {
  arg: 'containers',
  type: 'object',
  root: true
};

PostgresStorage.prototype.getContainer.http = {
  verb: 'get',
  path: '/:container'
};

PostgresStorage.prototype.destroyContainer.shared = true;

PostgresStorage.prototype.destroyContainer.accepts = [
  {
    arg: 'container',
    type: 'string'
  }
];

PostgresStorage.prototype.destroyContainer.returns = {};

PostgresStorage.prototype.destroyContainer.http = {
  verb: 'delete',
  path: '/:container'
};

PostgresStorage.prototype.upload.shared = true;

PostgresStorage.prototype.upload.accepts = [
  {
    arg: 'container',
    type: 'string'
  }, {
    arg: 'req',
    type: 'object',
    http: {
      source: 'req'
    }
  }, {
    arg: 'res',
    type: 'object',
    http: {
      source: 'res'
    }
  }
];

PostgresStorage.prototype.upload.returns = {
  arg: 'result',
  type: 'object'
};

PostgresStorage.prototype.upload.http = {
  verb: 'post',
  path: '/:container/upload'
};

PostgresStorage.prototype.getFiles.shared = true;

PostgresStorage.prototype.getFiles.accepts = [
  {
    arg: 'container',
    type: 'string'
  }
];

PostgresStorage.prototype.getFiles.returns = {
  arg: 'file',
  type: 'array',
  root: true
};

PostgresStorage.prototype.getFiles.http = {
  verb: 'get',
  path: '/:container/files'
};

PostgresStorage.prototype.getFile.shared = true;

PostgresStorage.prototype.getFile.accepts = [
  {
    arg: 'container',
    type: 'string'
  }, {
    arg: 'file',
    type: 'string'
  }
];

PostgresStorage.prototype.getFile.returns = {
  arg: 'file',
  type: 'object',
  root: true
};

PostgresStorage.prototype.getFile.http = {
  verb: 'get',
  path: '/:container/files/:file'
};

PostgresStorage.prototype.removeFile.shared = true;

PostgresStorage.prototype.removeFile.accepts = [
  {
    arg: 'container',
    type: 'string'
  }, {
    arg: 'file',
    type: 'string'
  }
];

PostgresStorage.prototype.removeFile.returns = {};

PostgresStorage.prototype.removeFile.http = {
  verb: 'delete',
  path: '/:container/files/:file'
};

PostgresStorage.prototype.download.shared = true;

PostgresStorage.prototype.download.accepts = [
  {
    arg: 'container',
    type: 'string'
  }, {
    arg: 'file',
    type: 'string'
  }, {
    arg: 'res',
    type: 'object',
    http: {
      source: 'res'
    }
  }
];

PostgresStorage.prototype.download.http = {
  verb: 'get',
  path: '/:container/download/:file'
};

exports.initialize = function(dataSource, callback) {
  var connector, k, m, method, opt, ref, settings;
  settings = dataSource.settings || {};
  connector = new PostgresStorage(settings);
  dataSource.connector = connector;
  dataSource.connector.dataSource = dataSource;
  connector.DataAccessObject = function() {};
  ref = PostgresStorage.prototype;
  for (m in ref) {
    method = ref[m];
    if (_.isFunction(method)) {
      connector.DataAccessObject[m] = method.bind(connector);
      for (k in method) {
        opt = method[k];
        connector.DataAccessObject[m][k] = opt;
      }
    }
  }
  connector.define = function(model, properties, settings) {};
  if (callback) {
    dataSource.connector.connect(callback);
  }
};
