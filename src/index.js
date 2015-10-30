'use strict';

module.exports = tileReduce;

var bboxPolygon = require('turf-bbox-polygon');
var normalize = require('geojson-normalize');
var tilecover = require('tile-cover');
var ProgressBar = require('progress');

var EventEmitter = require('events').EventEmitter;
var cpus = require('os').cpus().length;
var fork = require('child_process').fork;
var path = require('path');

function tileReduce(options) {
  var tiles = parseToTiles(options, options.zoom);
  var remaining = tiles.length;
  var total = remaining;
  var workers = [];
  var workersReady = 0;
  var autoShutdown = false;
  var ee = new EventEmitter();
  var bar = new ProgressBar(':current / :total tiles (:percent), :elapseds elapsed [:bar] ', {total: remaining});

  for (var i = 0; i < cpus - 1; i++) {
    var worker = fork(path.join(__dirname, 'worker.js'), [options.map, JSON.stringify(options.sources)]);
    worker.on('message', handleMessage);
    workers.push(worker);
  }

  function handleMessage(message) {
    if (message.ready && ++workersReady === workers.length) {
      ee.emit('start', tiles);
      sendTiles(tiles);
    } else if (message.reduce !== undefined) {
      bar.tick();
      if (message.reduce !== null) ee.emit('reduce', message.reduce);
      if (--remaining === 0 && autoShutdown) shutdown();
    }
  }

  function shutdown() {
    while (workers.length) workers.pop().kill();
    ee.emit('end');
  }

  function sendTiles(tiles) {
    for (var i = 0; i < tiles.length; i++) {
      workers[i % workers.length].send(tiles[i]);
    }
  }

  ee.run = function (runoption) {
    var tileList = parseToTiles(runoption, options.zoom);
    remaining += tileList.length;
    total += tileList.length;
    bar.width = bar.total = total; // reset bar width

    // If workers are all ready, go ahead and queue these up. 
    if (workersReady === workers.length) {
      sendTiles(tileList);
    } else {
      // Otherwise, we haven't done `sendTiles` yet, so go ahead and add these tiles
      // to the full list
      tiles = tiles.concat(tileList);
    }
  }

  ee.end = function () {
    autoShutdown = true;

    // If we finished processing everything and we're sending `end` way after
    // the fact, autoShutdown won't get triggered, so we end it here.
    if (remaining === 0 && workersReady === workers.length) shutdown();
  }

  return ee;
}

function parseToTiles(options, zoom) {
  var poly;

  if (options.tiles) return options.tiles;

  if (options.bbox) {
    poly = bboxPolygon(options.bbox);
  } else if (options.geojson) {
    poly = options.geojson;
  } else {
    // No job area specified. 
    return [];
  }

  return tilecover.tiles(normalize(poly).features[0].geometry, {min_zoom: zoom, max_zoom: zoom});
}
