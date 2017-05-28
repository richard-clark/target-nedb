const Ajv = require("ajv");
const Datastore = require("nedb");
const path = require("path");
const split = require("split");

module.exports = function nedbTarget(dbBasePath="./", echo=false, stream=process.stdin) {

  let schemasByStream = {};
  let keysPropertiesByStream = {};
  let datastoresByStream = {};
  let queue = [];
  let processing = false;
  stream.pipe(split())
    .on("data", (line) => _handleLine(line));

  function _handleLine(line) {
    if (line.length > 0) {
      queue.push(line);
      _processQueue();
    }
    if (echo) {
      process.stdout.write(line + "\n");
    }
  }

  function _processSchema(data, callback) {
    schemasByStream[data.stream] = data.schema;
    keysPropertiesByStream[data.stream] = data.key_properties;
    if (!data.key_properties) {
      throw new Error("Missing key_properties");
    }
    callback();
  }

  function _processRecord(data, callback) {
    const schema = schemasByStream[data.stream];
    if (!schema) {
      throw new Error(`No schema for stream '${data.stream}'`);
    }

    const ajv = new Ajv();
    const validator = ajv.compile(schema);
    const record = data.record;
    if (!validator(record)) {
      throw new Error(`Validation failed for record: '${JSON.stringify(data)}' -- errors: '${JSON.stringify(validator.errors)}'`);
    }

    let _id = null;
    let keyProperties = keysPropertiesByStream[data.stream]
    if (keyProperties) {
      _id = keyProperties.map((key) => record[key]).join("");
    }

    let datastore = datastoresByStream[data.stream];
    if (!datastore) {
      datastore = new Datastore({
        filename: path.join(dbBasePath, `${data.stream}.db`),
        autoload: true
      });
      datastoresByStream[data.stream] = datastore;
    }

    if (_id) {
      datastore.update(
        {_id},
        Object.assign(record, {_id}),
        {upsert: true},
        callback
      );
    } else {
      datastore.insert(record, callback);
    }
  }

  function _processQueue() {
    if (processing || queue.length === 0) {
      return;
    }
    processing = true;
    const [line] = queue.splice(0, 1);
    const data = JSON.parse(line);
    function _callback() {
      processing = false;
      _processQueue();
    }

    if (data.type === "SCHEMA") {
      _processSchema(data, _callback);
    } else if (data.type === "RECORD") {
      _processRecord(data, _callback);
    } else {
      _callback();
    }

  }
}
