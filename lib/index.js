var Promise = require('bluebird');
var _ = require('underscore');

var FilterESCompiler = require('./filter-es-compiler');
var juttle_utils = require('juttle/lib/runtime').utils;
var Orestes = require('orestes/src/orestes');
var Base = require('extendable-base');
var Heap = require('heap');

var connected = false;
var MAX_SERIES;
var MAX_POINTS;
var STREAM_FETCH_THRESHOLD;
var MAX_CONCURRENT_REQUESTS;

var stream = Base.extend({
    initialize: function(fetcher, fetch_size) {
        this.fetcher = fetcher;
        this.fetch_size = fetch_size;
        this.buffer = [];
        this._need_fetch = true;
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

    done: function() {
        return this.buffer.length === 0;
    },

    need_fetch: function() {
        return this._need_fetch;
    },

    fetch: function fetch() {
        var self = this;
        return this.fetcher.fetch(this.fetch_size)
        .then(function(result) {
            var points = result.points.map(function(pt) {
                return _.extend({time: pt[0], value: pt[1]}, self.fetcher.tags);
            });
            self.buffer = self.buffer.concat(points);
            self._need_fetch = !result.eof;
        });
    }
});

function ESBackend(config, JuttleRuntime) {
    Orestes.init(config);
    MAX_SERIES = config.max_series || 20000;
    MAX_POINTS = config.max_simultaneous_points || 100000;
    MAX_CONCURRENT_REQUESTS = config.MAX_CONCURRENT_REQUESTS || 100;
    STREAM_FETCH_THRESHOLD = config.STREAM_FETCH_THRESHOLD || 3;

    var connect_promise = Orestes.connect(config);

    var Read = JuttleRuntime.proc.base.extend({
        procName: 'orestes_read',
        sourceType: 'batch',

        initialize: function(options, params) {
            this._setup_time_filter(options);
            var filter_ast = params.filter_ast;
            if (filter_ast) {
                var filter_es_compiler = new FilterESCompiler();
                var result = filter_es_compiler.compile(filter_ast);
                this.es_filter = result.filter;
            }

            this.es_opts = {};
            this.space = options.space || 'default';
        },

        _setup_time_filter: function(options) {
            this.now = this.program.now;

            if (options.from && options.to) {
                this.from = options.from;
                this.to = options.to;
            } else if (options.last) {
                this.to = this.now;
                this.from = this.to.subtract(options.last);
            } else {
                throw new Error('-from/-to or -last time filter required');
            }
        },

        start: function() {
            var self = this;
            var fetchers = [];
            return connect_promise.then(function() {
                var startMs = self.from.milliseconds();
                var endMs = self.to.milliseconds();
                var options = {series_limit: MAX_SERIES};

                function process_fetcher(fetcher) {
                    fetchers.push(fetcher);
                }

                return Orestes.read(self.es_filter, self.space, startMs, endMs, options, process_fetcher);
            })
            .then(function() {
                var heap = new Heap(function(s1, s2) {
                    return s1.peek_time() - s2.peek_time();
                });
                var fetch_size = MAX_POINTS / fetchers.length;
                var streams = fetchers.map(function(fetcher) {
                    return new stream(fetcher, fetch_size);
                });

                function drop_empties() {
                    streams = streams.filter(function(s) {
                        return !s.done() || s.need_fetch();
                    });
                }

                // Here's our main loop.
                // The heap holds a bunch of stream objects, organized so
                // that we can quickly access the stream with the earliest
                // point.  Note that the first time through the loop, the
                // heap is empty so we go straight to the fetch logic to
                // get the heap populated.  Once that is done, each iteration
                // of the loop emits as many points as it can, then fetches
                // a bunch more.  We do that over and over until there are
                // no more points...
                function loop() {

                    var out = [];
                    drop_empties();

                    // try to emit some points.  but only if we have
                    // some points for every stream
                    var somebody_needs_fetch = streams.some(function(stream) {
                        return stream.done();
                    });
                    if (!somebody_needs_fetch) {
                        heap.clear();
                        streams.forEach(function(stream) {
                            heap.push(stream);
                        });
                        streams = [];

                        while (heap.size() > 0) {
                            var strm = heap.pop();
                            var pt;

                            var next = heap.peek();
                            var next_time = next ? next.peek_time() : Infinity;

                            while (!strm.done() && strm.peek_time() <= next_time) {
                                pt = strm.pop();
                                out.push(pt);
                            }

                            // if we still have some points, then we just cycle
                            // through the while loop again, grabbing points
                            // from other streams
                            if (!strm.done()) {
                                heap.push(strm);
                            }
                            // if this stream just needs more points, then
                            // we need to do more I/O before continuing.
                            else if (strm.need_fetch()) {
                                streams = heap.toArray();
                                streams.push(strm);
                                break;
                            }
                            // otherwise, this stream is done, just drop it out
                            // of the heap and continue
                        }
                    }

                    var promise;

                    drop_empties();

                    if (streams.length === 0) {
                        promise = Promise.resolve();
                    } else {
                        var tofetch = streams.filter(function(stream) {
                            return stream.need_fetch() &&
                                stream.buffer.length < STREAM_FETCH_THRESHOLD;
                        });

                        if (tofetch.length === 0) {
                            throw new Error('uh oh, tofetch should not be empty');
                        }

                        promise = Promise.map(tofetch, function(stream) {
                            return stream.fetch();
                        }, {concurrency: MAX_CONCURRENT_REQUESTS})
                        .then(function() {
                            if (!self.cancelled) {
                                return loop();
                            }
                        });
                    }

                    if (out.length > 0) {
                        self.emit(juttle_utils.toNative(out));
                    }

                    return promise;
                }

                return loop();
            })
            .then(function() {
                self.emit_eof();
            })
            .catch(function(err) {
                self.trigger('error', err);
                self.emit_eof();
            });
        }
    });

    var Write = JuttleRuntime.proc.fanin.extend({
        procName: 'orestes_write',
        initialize: function(options) {
            var self = this;
            this.isSink = true;
            this.eofs = 0;
            this.space = options.space || 'default';
            this.done = new Promise(function(resolve, reject) {
                // totally opaque promise that juttle-test-utils.run_juttle depends on
                // to wait for the write to finish
                self.resolve = resolve;
                self.reject = reject;
            });
        },
        process: function(points) {
            var self = this;
            return connect_promise.then(function() {
                return Orestes.write(points, self.space);
            })
            .catch(function(err) {
                self.trigger('error', err);
            });
        },
        eof: function(from) {
            this.eofs++;
            if (this.eofs === this.ins.length) {
                this.resolve(this);
                this.emit_eof();
            }
        }
    });

    return {
        name: 'orestes',
        read: Read,
        write: Write
    };
}

module.exports = ESBackend;
