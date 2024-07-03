var sqlite3 = require('sqlite3');
var mkdirp = require('mkdirp');
const fs = require('fs').promises;
const path = require('path');
const { GetObjectCommand, PutObjectCommand, S3Client } = require("@aws-sdk/client-s3");

const dbfile = './var/db/todos.db';

const client = new S3Client();

// fetch database file from Tigris; if the database does not yet exit,
// create one and push it to Tigris
async function get() {
  if (!process.env.BUCKET_NAME) return;

  const getCommand = new GetObjectCommand({
    Bucket: process.env.BUCKET_NAME,
    Key: path.basename(dbfile),
  });

  try {
    const response = await client.send(getCommand);
    await fs.writeFile(dbfile, await response.Body);
  } catch (err) {
    if (err.name !== 'NoSuchKey') {
      throw err;
    } else {
      mkdirp.sync(path.dirname(dbfile));
      const db = new sqlite3.Database(dbfile);

      await new Promise((resolve, reject) => {
        db.serialize(function () {
          db.run("CREATE TABLE IF NOT EXISTS todos ( \
            id INTEGER PRIMARY KEY, \
            title TEXT NOT NULL, \
            completed INTEGER \
          )", [], () => {put().then(resolve, reject)});
        })
      })
    }
  }
}

// push database file to Tigris
async function put() {
  if (!process.env.BUCKET_NAME) return;

  const putCommand = new PutObjectCommand({
    Bucket: process.env.BUCKET_NAME,
    Key: path.basename(dbfile),
    Body: await fs.readFile(dbfile),
  });

  const response = await client.send(putCommand);
}

module.exports = {
  // query support: fetch database file from Tigris, query the database,
  // call callback
  all: async function (query, params, callback) {
    await get();

    const db = new sqlite3.Database(dbfile);

    db.all(query, params, callback);
  },

  // update support: fetch database file from Tigris, update the database,
  // push the database file to Tigris, call callback
  run: async function (query, params, callback) {
    await get();

    const db = new sqlite3.Database(dbfile);

    db.run(query, params, () => {
      put().then(callback)
    })
  }
}
