# target-nedb

Singer.io Target for [NeDB](https://github.com/louischatriot/nedb), a lightweight MongoDB-like database for Node.js.

Compatible with any Tap that implements the [Singer Specification](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

Features:

- Each stream type will be placed in its own collection (its own file) in a directory you specify.

- If a stream type has multiple keys, the key values will be treated as strings and concatenated together to form a single primary key.

## Installation

```shell
npm i -g nedb-target
```

## Usage

```shell
get_singer_stream_somehow | nedb-target data/
```

Use `nedb-target --help` for full usage information.

## Testing

Checkout the repo and run `npm install` to install development dependencies.

Run tests with `npm test`.
