# nedb-target

Singer.io Target for [NeDB](https://github.com/louischatriot/nedb), a lightweight MongoDB-like database for Node.JS.

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