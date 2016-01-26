var Read = require('./read');
var Write = require('./write');
var utils = require('./orestes-adapter-utils');

function OrestesAdapter(config) {
    utils.init(config);
    Read.init(config);
    Write.init(config);

    return {
        name: 'orestes',
        read: Read.proc,
        write: Write.proc
    };
}

module.exports = OrestesAdapter;
