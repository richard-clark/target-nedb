const {Readable} = require("stream");
const assert = require('chai').assert;
const path = require("path");
const os = require("os");
const tmp = require("tmp");
const Datastore = require("nedb");
const nedbTarget = require("./nedbTarget.js");

const EXPECTED_USERS = [
  { id: 1, name: 'Chris', _id: '1' },
  { id: 2, name: 'Mike', _id: '2' }
];
const EXPECTED_LOCATIONS = [
  { id: 1, name: 'Philadelphia', _id: '1' }
]

function asyncNedbTarget(opts) {
  return new Promise((resolve, reject) => {
    nedbTarget(opts, (error) => {
      if (error) { return reject(error); }
      resolve();
    });
  });
}

function queryAllAsync(datastore) {
  return new Promise((resolve, reject) => {
    datastore.find({}, (err, result) => {
      if (err) { return reject(err); }
      resolve(result);
    });
  });
}

function invokeWithTempDir(callback) {
  return new Promise((resolve, reject) => {
    tmp.dir({
      unsafeCleanup: true
    }, function _tempDirCreated(err, tmpDir, cleanupCallback) {
      if (err) throw err;
      callback(tmpDir).then((result) => {
        cleanupCallback();
        resolve(result);
      })
      .catch((error) => {
        cleanupCallback();
        reject(error);
      });
    });
  });
}

async function testSuccessful(tmpDir) {
  const LINES = [
    `{"type": "SCHEMA", "stream": "users", "key_properties": ["id"], "schema": {"required": ["id"], "type": "object", "properties": {"id": {"type": "integer"}}}}`,
    `{"type": "RECORD", "stream": "users", "record": {"id": 1, "name": "Chris"}}`,
    `{"type": "RECORD", "stream": "users", "record": {"id": 2, "name": "Mike"}}`,
    `{"type": "SCHEMA", "stream": "locations", "key_properties": ["id"], "schema": {"required": ["id"], "type": "object", "properties": {"id": {"type": "integer"}}}}`,
    `{"type": "RECORD", "stream": "locations", "record": {"id": 1, "name": "Philadelphia"}}`,
    `{"type": "STATE", "value": {"users": 2, "locations": 1}}`
  ];

  let inputStream = new Readable();
  inputStream.push(LINES.join("\n") + "\n");
  inputStream.push(null);

  await asyncNedbTarget({
    dbBasePath: tmpDir,
    inputStream
  })

  const userStore = new Datastore({
    filename: path.join(tmpDir, "users.db"),
    autoload: true
  });
  const users = await queryAllAsync(userStore);

  const locationStore = new Datastore({
    filename: path.join(tmpDir, "locations.db"),
    autoload: true
  });
  const locations = await queryAllAsync(locationStore);

  return {users, locations};

}

async function testInvalidSchema(tmpDir) {
  const LINES = [
    `{"type": "SCHEMA", "stream": "users", "key_properties": ["id"], "schema": {"required": ["id"], "type": "object", "properties": {"id": {"type": "integer"}}}}`,
    `{"type": "RECORD", "stream": "users", "record": {"id": "1", "name": "Chris"}}`,
    `{"type": "RECORD", "stream": "users", "record": {"id": 2, "name": "Mike"}}`,
    `{"type": "SCHEMA", "stream": "locations", "key_properties": ["id"], "schema": {"required": ["id"], "type": "object", "properties": {"id": {"type": "integer"}}}}`,
    `{"type": "RECORD", "stream": "locations", "record": {"id": "1", "name": "Philadelphia"}}`,
    `{"type": "STATE", "value": {"users": 2, "locations": 1}}`
  ];

  let inputStream = new Readable();
  inputStream.push(LINES.join("\n") + "\n");
  inputStream.push(null);

  await asyncNedbTarget({
    dbBasePath: tmpDir,
    inputStream
  })
}

async function testMissingSchema(tmpDir) {
  const LINES = [
    `{"type": "RECORD", "stream": "users", "record": {"id": 1, "name": "Chris"}}`,
    `{"type": "RECORD", "stream": "users", "record": {"id": 2, "name": "Mike"}}`,
    `{"type": "SCHEMA", "stream": "locations", "key_properties": ["id"], "schema": {"required": ["id"], "type": "object", "properties": {"id": {"type": "integer"}}}}`,
    `{"type": "RECORD", "stream": "locations", "record": {"id": "1", "name": "Philadelphia"}}`,
    `{"type": "STATE", "value": {"users": 2, "locations": 1}}`
  ];

  let inputStream = new Readable();
  inputStream.push(LINES.join("\n") + "\n");
  inputStream.push(null);

  await asyncNedbTarget({
    dbBasePath: tmpDir,
    inputStream
  })
}
describe("end-to-end", function() {

  it("populates collections with the expected data", function(){

    return invokeWithTempDir(testSuccessful)
    .then((data) => {
      assert.deepEqual(data.users, EXPECTED_USERS, "users match expected");
      assert.deepEqual(data.locations, EXPECTED_LOCATIONS, "locations match expected");
    });

  });


  it("throws an error for an invalid schema", function(){

    return invokeWithTempDir(testInvalidSchema)
    .then(() => {
      throw new Error("Expected promise to be rejected");
    }).catch((error) => {
      const EXPECTED_ERROR_MESSAGE = `Validation failed for record: \'{"type":"RECORD","stream":"users","record":{"id":"1","name":"Chris"}}\' -- errors: \'[{"keyword":"type","dataPath":".id","schemaPath":"#/properties/id/type","params":{"type":"integer"},"message":"should be integer"}]\'`;
      assert.instanceOf(error, Error);
      assert.equal(error.message, EXPECTED_ERROR_MESSAGE);
    });

  });

  it("throws an error for a record without a schema", function(){

    return invokeWithTempDir(testMissingSchema)
    .then(() => {
      throw new Error("Expected promise to be rejected");
    }).catch((error) => {
      const EXPECTED_ERROR_MESSAGE = `No schema for stream 'users'`;
      assert.instanceOf(error, Error);
      assert.equal(error.message, EXPECTED_ERROR_MESSAGE);
    });

  });

});
