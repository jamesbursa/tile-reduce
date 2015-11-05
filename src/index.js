'use strict';

module.exports = tileReduce;

var ProgressBar = require('progress');

var EventEmitter = require('events').EventEmitter;
var cpus = require('os').cpus().length;
var fork = require('child_process').fork;
var path = require('path');
var fs = require('fs');
var split = require('split');
var cover = require('./cover');
var streamArray = require('stream-array');
var workStream = require('./work-stream');

var tileTransform = split(function(line) {
  return line.split(' ').map(Number);
});

function tileReduce(options) {
  var workers = [];
  var workersReady = 0;
  var tileStream = null;

  for (var i = 0; i < cpus - 1; i++) {
    //var worker = spawn('node', [path.join(__dirname, 'worker.js'), options.map, JSON.stringify(options.sources)], {stdio: ['pipe', 1, 2, 'ipc']});
    var worker = fork(path.join(__dirname, 'worker.js'), [options.map, JSON.stringify(options.sources)], {silent: true});
    worker.stdout.pipe(process.stdout);
    worker.once('message', workerReady); // startup message
    workers.push(worker);
  }


  var bar = new ProgressBar(':current / :total tiles (:percent), :elapseds elapsed [:bar] ', {
    total: 1,
    width: Infinity
  });
  bar.tick(0);

  var ee = new EventEmitter();
  var tiles = typeof options.tiles === 'string' ? null : cover(options);
  var workerstream = workStream(workers);

  function workerReady() { if (++workersReady === workers.length) run(); }

  function run() {
    ee.emit('start');

    if (tiles) {
      tileStream = streamArray(tiles);
      bar.total = tiles.length;
      bar.tick(0);
    } else {
      tileStream = fs.createReadStream(options.tiles);
      tileStream.pipe(tileTransform);
    }

    tileStream.pipe(workerstream)
      .on('data', reduce)
      .on('end', shutdown);
  }

  function reduce(message) {
    if (bar.total < message.tilesSent) {
      bar.total = message.tilesSent;
    }

    bar.tick();
    if (message.value !== null && message.value !== undefined) ee.emit('reduce', message.value);
  }

  function shutdown() {
    while (workers.length) {
      var worker = workers.pop();
      worker.stdout.unpipe();
      worker.kill();
    }
    ee.emit('end');
  }

  return ee;
}
