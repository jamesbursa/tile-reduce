var through2 = require('through2');
var queue = require('queue-async');

module.exports = function (workers) {
  var queuedLimit = 1000;
  var tilesSent = 0;
  var tilesRecieved = 0;

  var streamHandler = function (chunk, enc, callback) {
    var worker = lowestQueue();

    // If we've overqueued, retry
    if (worker.queueLength > queuedLimit) {
      return setImmediate(function () { streamHandler(chunk, enc, callback); });
    }

    tilesSent++;
    worker.queueLength++;
    worker.stdin.write(JSON.stringify(chunk)+'\n');
    callback();
  };
 
  var streamFlush = function (callback) {
    stream.on('final-data', callback)
  } 
  var stream = through2.obj(streamHandler, streamFlush);

  for (var i = 0; i < workers.length; i++) {
    workers[i].queueLength = 0;
    workers[i].on('message', function(message) {
      if (!message.reduce) return;
      this.queueLength--;
      stream.push(message);
      if (++tilesRecieved >= tilesSent && tilesSent > 0) {
        stream.end();
        stream.emit('final-data')
      }
    });
  }

  var lowestQueue = function() {
    var min = Infinity;
    var lowest = null;
    for (var i = 0; i < workers.length; i++) {
      // If this one is zero, go ahead and pick it.
      if (workers[i].queueLength === 0) return workers[i];
      if (workers[i].queueLength < min) {
        min = workers[i].queueLength;
        lowest = workers[i];
      }
    }
    return lowest;
  }

  return stream;
}