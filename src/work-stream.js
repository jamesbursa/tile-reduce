'use strict';

var through2 = require('through2');

module.exports = function(workers) {
  var tilesSent = 0;
  var tilesReceived = 0;

  var stream = through2.obj({allowHalfOpen: true},
    // ._transform function. Handles piped data
    function(chunk, enc, callback) {
      var worker = workers[tilesSent % workers.length];

      tilesSent++;
      var stat = worker.stdin.write(JSON.stringify(chunk) + '\n');

      // wait for drain if this worker is overloaded. Technically, this will
      // block all workers. but since we split work ~evenly it shouldn't be a problem
      if (!stat) worker.stdin.once('drain', callback);
      else callback();
    },
    function(callback) {
      // TODO: making up an event and tacking it onto this stream feels weird and bad
      stream.on('final-data', callback);
    }
  );

  // handles messages back from workers
  function handleMessage(message) {
    if (!message.reduce) return;
    stream.push({value: message.value, tilesSent: tilesSent});
    if (++tilesReceived >= tilesSent && tilesSent > 0) {
      stream.end();
      stream.emit('final-data');
    }
  }

  for (var i = 0; i < workers.length; i++) {
    workers[i].on('message', handleMessage);
  }

  return stream;
};
