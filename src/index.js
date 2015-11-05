'use strict';

module.exports = tileReduce;

var ProgressBar = require('progress');

var EventEmitter = require('events').EventEmitter;
var cpus = require('os').cpus().length;
var spawn = require('child_process').spawn;
var path = require('path');
var fs = require('fs');
var split = require('split');
var cover = require('./cover');
var streamArray = require('stream-array');
var WorkStream = require('./work-stream');

var tileTransform = split(function(line) {
  return line.split(' ').map(Number);
});

function tileReduce(options) {
  var workers = [];
  var workersReady = 0;
  var tileStream = null;

  for (var i = 0; i < cpus - 1; i++) {
    var worker = spawn('node', [path.join(__dirname, 'worker.js'), options.map, JSON.stringify(options.sources)], {stdio: ['pipe', 1, 2, 'ipc']});
    worker.once('message', handleMessage);
    workers.push(worker);
  }

  var workstream = WorkStream(workers);

  function handleMessage(message) {
    if (message.ready && ++workersReady === workers.length) run();
  }

  var bar = new ProgressBar(':current / :total tiles (:percent), :elapseds elapsed [:bar] ', {
    total: 1,
    width: Infinity
  });
  bar.tick(0);

  var ee = new EventEmitter();
  var tiles = typeof options.tiles === 'string' ? null : cover(options);


  function run() {
    ee.emit('start');

    if (tiles) {
      tileStream = streamArray(tiles).on('data', handleTile);
      bar.total = tiles.length;
      bar.tick(0);
    } else {
      tileStream = fs.createReadStream(options.tiles);
      tileStream.pipe(tileTransform).pipe(workstream);
    }

    workstream.on('data', reduce).on('finish', shutdown);
  }


  function reduce(value) {
    bar.total++
    bar.tick();
    if (value !== null && value !== undefined) ee.emit('reduce', value);
  }

  function shutdown() {
    while (workers.length) workers.pop().kill();
    ee.emit('end');
  }

  return ee;
}
