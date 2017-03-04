var mysql = require('mysql');
var q = require('q');
var _ = require('lodash');
var argv = require('yargs').argv;
var pool;

function setConfig(config) {
  config.host = argv.mysql_host || config.host;
  config.user = argv.mysql_user || config.user;
  config.password = argv.mysql_password || config.password;
  console.log('mysql initialised with host "' + config.host + '" and user "' + config.user + '"');
  pool = mysql.createPool(config);
}

function getConnection() {
  var deferred = q.defer();

  pool.getConnection(function (err, connection) {
        if (err) {
          deferred.reject(err);
        } else {
          deferred.resolve(connection);
        }
      });

  return deferred.promise;
}

function endConnection(connection, onSuccess) {
  var deferred = q.defer();

  connection.end(function(err) {
    if (err) {
      console.error(err);
    }
    if (onSuccess) {
      onSuccess();
    }
    deferred.resolve();
  });

  return deferred.promise;
}

function beginTransaction(connection) {
  var deferred = q.defer();

  connection.beginTransaction(function(err) {
    if (err) {
      deferred.reject(err);
    } else {
      deferred.resolve();
    }
  });

  return deferred.promise;
}

function commitTransaction(connection) {
  var deferred = q.defer();

  connection.commit(function(err) {
    if (err) {
      connection.rollback(function() {
        deferred.reject(err);
      });
    }
    deferred.resolve();
  });

  return deferred.promise;
}

function queryWithConnection(connection, queryText, params, forceSuccess) {
  var deferred = q.defer();

  var _query = _.bind(connection.query, connection);
  queryText = mysql.format(queryText, params || []);
  _query(queryText, function(err, result, fields) {
    if (err) {
      console.log('error for query ' + queryText);
      console.log('error: ' + err);
      if (forceSuccess) {
        deferred.resolve(null, null);
      } else {
        deferred.reject(err);
      }
    } else {
      deferred.resolve(result, fields);
    }
  });
  return deferred.promise;
}

function query(queryText, params, force) {
  var deferred = q.defer();

  pool.getConnection(function (err, connection) {
    if (err) {
      console.error(err);
      deferred.reject(err);
    } else {
      var _query = _.bind(connection.query, connection);
      queryText = mysql.format(queryText, params || []);
      _query(queryText, function(err, result, fields) {
        try {
          connection.release();
          if (err) {
            console.error('error for query ' + queryText);
            console.error(err);
          }
          if (force || !err) {
            deferred.resolve(result, fields);
          }
        } catch (err2) {
          console.error('error for query ' + queryText);
          console.error('error: ' + err2);
          deferred.reject(err2);
        }
      });
    }

  });
  return deferred.promise;
}

function runScript(scriptText, params) {
  var promise = q.when([]);

  var queries = scriptText.split(';');

  for (var i = 0; i < queries.length; ++i) {
    var queryText = queries[i].replace('\n', '');
    if (queryText.trim().length > 0) {
      promise = promise.then(_.partial(query, queryText, params));
    }
  }

  return promise;
}

function nonLockingQuery(queryText, params, forceSuccess) {
  var deferred = q.defer();

  pool.getConnection(function(err, connection) {
    if (err) {
      console.error(err);
      deferred.reject(err);
    } else {
      var _query = _.bind(connection.query, connection);

      _query('SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;', null, function(err1, result1, fields) {
        if (err1) {
          console.log('error: ' + err1);
          deferred.reject(err1);
          connection.release();
        } else {
          _query(queryText, params, function(err2, result, fields) {
            if (err2) {
              console.error('error for query ' + queryText + ' -- ' + JSON.stringify(params));
              console.error('error: ' + err2);
              deferred.reject(err2);
              connection.release();
            } else {
              _query('SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;', null, function(err3, result3, fields) {
                try {
                  deferred.resolve(result, fields);
                  connection.release();
                } catch (err4) {
                  console.error('error for query ' + queryText + ' -- ' + JSON.stringify(params));
                  console.error('error: ' + err4);
                  deferred.reject(err4);
                  connection.release();
                }
              });
            }
          });
        }
      });
    }
  });

  return deferred.promise;
}

module.exports = {
  setConfig: setConfig,
  getConnection: getConnection,
  endConnection: endConnection,
  beginTransaction: beginTransaction,
  commitTransaction: commitTransaction,
  queryWithConnection: queryWithConnection,
  nonLockingQuery: nonLockingQuery,
  query: query,
  runScript: runScript
};
