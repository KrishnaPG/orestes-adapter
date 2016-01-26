# Juttle Orestes Adapter

[![Build Status](https://travis-ci.org/juttle/juttle-orestes-adapter.svg)](https://travis-ci.org/juttle/orestes-adapter)

The Juttle Orestes Adapter enables interaction with [Orestes](https://github.com/juttle/orestes).

## Examples

Write a point with value 1 and name `orestes_test`.

```juttle
emit -limit 1
| put value = 1, name='orestes_test'
| write orestes
```
Read all points in the last hour with name `orestes_test`.

```juttle
read orestes -last :hour: name='orestes_test'
```

## Installation

Like Juttle itself, the adapter is installed as a npm package. Both Juttle and
the adapter need to be installed side-by-side:

```bash
$ npm install juttle
$ npm install juttle-orestes-adapter
```

## Configuration

The adapter needs to be registered and configured so that it can be used from
within Juttle. To do so, add the following to your `~/.juttle/config.json` file:

```json
{
    "adapters": {
        "juttle-orestes-adapter": {
            "cassandra": {
                "host": "127.0.0.1",
                "native_transport_port": 9042
            },
            "elasticsearch": {
                "host": "127.0.0.1",
                "port": 9200
            }
        },
        "spaces": {
            "default": {
                "table_granularity_days": 1
            }
        }
    }
}
```

That will run Orestes with a space called "default". Orestes will talk to Cassandra on localhost:9042 and Elasticsearch on localhost:9200.

## Usage

### Read options


Name | Type | Required | Description | Default
-----|------|----------|-------------|---------
`from` | moment | no | select points after this time (inclusive) | none, either `-from` and `-to` or `-last` must be specified
`to`   | moment | no | select points before this time (exclusive) | none, either `-from` and `-to` or `-last` must be specified
`last` | duration | no | select points within this time in the past (exclusive) | none, either `-from` and `-to` or `-last` must be specified

## Contributing

Want to contribute? Awesome! Don’t hesitate to file an issue or open a pull
request.
