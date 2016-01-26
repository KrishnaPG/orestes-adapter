var Orestes = require('orestes/lib/orestes');
var _startup_promise = Promise.reject(new Error('orestes not started'));

function init(config) {
    Orestes.init(config);
    _startup_promise = Orestes.startup(config);
}

function startup() {
    return _startup_promise;
}

module.exports = {
    init: init,
    startup: startup
};
