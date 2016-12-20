/**
 * A Connection handler for Amazon ES.
 *
 * Uses the aws-sdk to make signed requests to an Amazon ES endpoint.
 * Define the Amazon ES config and the connection handler
 * in the client configuration:
 */

var AWS = require('aws-sdk');
var zlib = require('zlib');

var awsReq = function(host, reqOpts, region, logger, debug, cb) {
  var endpoint = new AWS.Endpoint(host);

  var incoming;
  var timeoutId;
  var request;
  var req;
  var status = 0;
  var headers = {};
  var response;

  // general clean-up procedure to run after the request
  // completes, has an error, or is aborted.
  var cleanUp = function (err) {
    clearTimeout(timeoutId);

    req && req.removeAllListeners();
    incoming && incoming.removeAllListeners();

    if ((err instanceof Error) === false) {
      err = void 0;
    }

    if (debug) {
      logger.log('debug', JSON.stringify({
        params: reqOpts,
        response: response,
        status: status
      }));
    }

    if (err) {
      cb(err);
    } else {
      cb(err, response, status, headers);
    }
  }

  request = new AWS.HttpRequest(endpoint);

  // copy across options
  for (var opt in reqOpts) {
    request[opt] = reqOpts[opt];
  }
  request.region = region;
  if (!request.headers) request.headers = {};
  request.headers['presigned-expires'] = false;
  request.headers['Host'] = endpoint.host;

  // Sign the request (Sigv4)
  var signer = new AWS.Signers.V4(request, 'es');

  AWS.config.getCredentials(function(err) {
    if (err) return cb(err);

    signer.addAuthorization(AWS.config.credentials, new Date());

    var send = new AWS.NodeHttpClient();
    req = send.handleRequest(request, null, function (_incoming) {
      incoming = _incoming;
      status = incoming.statusCode;
      headers = incoming.headers;
      response = '';

      var encoding = (headers['content-encoding'] || '').toLowerCase();
      if (encoding === 'gzip' || encoding === 'deflate') {
        incoming = incoming.pipe(zlib.createUnzip());
      }

      incoming.setEncoding('utf8');
      incoming.on('data', function (d) {
        response += d;
      });

      incoming.on('error', cleanUp);
      incoming.on('end', cleanUp);
    }, cleanUp);

    req.on('error', cleanUp);

    req.setNoDelay(true);
    req.setSocketKeepAlive(true);
  });
}

module.exports = awsReq;
