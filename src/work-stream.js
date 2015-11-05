var through2 = require('through2');
var queue = require('queue-async');

module.exports = function (workers) {
  var queuedLimit = 1;
  var q = queue();

  var streamHandler = function (chunk, enc, callback) {
    var worker = lowestQueue();

    // If we've overqueued, retry
    if (worker.queueLength > queuedLimit) {
      return setTimeout(function () {streamHandler(chunk, enc, callback)},100);
    }
    
    worker.queueLength++;
    q.defer(function (worker, done) {
      worker.once('message', function (message) {
        worker.queueLength--;
        stream.push(message);
        done();
      });
      worker.send(chunk);
    }, worker);
    callback();
  };
  
  var stream = through2.obj(streamHandler);

  stream._flush = function (callback) {
    q.awaitAll(callback);
  };


  for (var i = 0; i < workers.length; i++) {
    workers[i].queueLength = 0;
    // workers[i].on('message', handleMessage);
  }

  // function handleMessage (message) {
  //   this.queuedLength--;
  //   stream.push(message);
  // }

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