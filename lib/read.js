'use strict';

var Promise = require('bluebird');
var _ = require('underscore');
var Heap = require('heap');
var Base = require('extendable-base');
var Juttle = require('juttle/lib/runtime').Juttle;
var juttle_utils = require('juttle/lib/runtime').utils;
var Orestes = require('orestes/lib/orestes');
var AdapterRead = require('juttle/lib/runtime/adapter-read');

var FilterESCompiler = require('./filter-es-compiler');
var utils = require('./orestes-adapter-utils');

var MAX_SERIES;
var MAX_POINTS;
var STREAM_FETCH_THRESHOLD;
var MAX_CONCURRENT_REQUESTS;

function init(config) {
    MAX_SERIES = config.max_series || 20000;
    MAX_POINTS = config.max_simultaneous_points || 100000;
    MAX_CONCURRENT_REQUESTS = config.MAX_CONCURRENT_REQUESTS || 100;
    STREAM_FETCH_THRESHOLD = config.STREAM_FETCH_THRESHOLD || 3;
}

var stream = Base.extend({
    initialize: function(fetcher, fetch_size) {
        this.fetcher = fetcher;
        this.fetch_size = fetch_size;
        this.buffer = [];
        this._more_to_fetch = true;
    },

    peek_time: function() {
        if (this.buffer.length === 0) {
            throw new Error('peek_time on empty buffer!');
        }
        return this.buffer[0].time;
    },

    pop: function() {
        return this.buffer.shift();
    },

    empty: function() {
        return this.buffer.length === 0;
    },

    more_to_fetch: function() {
        return this._more_to_fetch;
    },

    fetch: function fetch() {
        var self = this;
        return this.fetcher.fetch(this.fetch_size)
        .then(function(result) {
            var points = result.points.map(function(pt) {
                return _.extend({time: pt[0] / 1000, value: pt[1]}, self.fetcher.tags);
            });
            self.buffer = self.buffer.concat(points);
            self._more_to_fetch = !result.eof;
        });
    }
});

function new_stream_heap(streams) {
    var heap = new Heap(function compare(s1, s2) {
        return s1.peek_time() - s2.peek_time();
    });

    streams.forEach(function(stream) {
        heap.push(stream);
    });

    return heap;
}

class ReadOrestes extends AdapterRead {
    static get timeRequired() { return true; }

    constructor(options, params) {
        super(options, params);
        this.setup_filter(params);
        this.space = options.space || 'default';
    }

    setup_filter(params) {
        var filter_ast = params.filter_ast;
        if (filter_ast) {
            var filter_es_compiler = new FilterESCompiler();
            var result = filter_es_compiler.compile(filter_ast);
            this.es_filter = result.filter;
        }
    }

    get_streams(from, to) {
        return utils.startup().then(() => {
            var startMs = from.milliseconds();
            var endMs = to.milliseconds();

            if (this.read_from === startMs && this.read_to === endMs) {
                return; // already called get_streams with this from/to so this.streams is set
            }

            this.read_from = startMs;
            this.read_to = endMs;

            var options = {series_limit: MAX_SERIES};
            var fetchers = [];

            return Orestes.read(this.es_filter, this.space, startMs, endMs, options, (fetcher) => {
                fetchers.push(fetcher);
            })
            .then(() => {
                var fetch_size = MAX_POINTS / fetchers.length;
                this.streams = fetchers.map(function(fetcher) {
                    return new stream(fetcher, fetch_size);
                });
            });
        })
        .then(() => {
            return this.fill_streams();
        });
    }

    fill_streams() {
        function needs_refill(stream) {
            return stream.more_to_fetch() &&
                stream.buffer.length < STREAM_FETCH_THRESHOLD;
        }

        if (this.streams.length === 0) {
            return Promise.resolve();
        } else {
            var streams_to_refill = this.streams.filter(needs_refill);

            if (streams_to_refill.length === 0) {
                throw new Error('uh oh, streams_to_refill should not be empty');
            }

            return Promise.map(streams_to_refill, (stream) => {
                return stream.fetch();
            }, {concurrency: MAX_CONCURRENT_REQUESTS});
        }
    }

    read_until_stream_empty_or_limit(limit) {
        var points = [];
        var heap = new_stream_heap(this.streams);

        while (heap.size() > 0) {
            var stream = heap.pop();

            var next = heap.peek();
            var next_time = next ? next.peek_time() : Infinity;

            while (!stream.empty() && stream.peek_time() <= next_time && points.length < limit) {
                points.push(stream.pop());
            }

            // if we still have some points, then we just cycle
            // through the while loop again, grabbing points
            // from another stream
            if (!stream.empty()) {
                heap.push(stream);
            } else if (stream.more_to_fetch() || points.length >= limit) {
                // if this stream needs more points, then we need
                // to do more I/O before continuing.
                this.streams = heap.toArray();
                this.streams.push(stream);
                break;
            }
            // otherwise, this stream is empty, just drop it out
            // of the heap and continue
        }

        return points;
    }

    read(from, to, limit) {
        return this.get_streams(from, to).then(() => {
            var points = this.read_until_stream_empty_or_limit(limit);

            this.streams = this.streams.filter((s) => {
                return !s.empty() || s.more_to_fetch();
            });

            var done = this.cancelled || this.streams.length === 0;

            return {
                readEnd: done ? to : null,
                points: juttle_utils.toNative(points)
            };
        });
    }
}

module.exports = {
    proc: ReadOrestes,
    init: init
};
