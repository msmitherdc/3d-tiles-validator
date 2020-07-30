'use strict';
const Cesium = require('cesium');
const fsExtra = require('fs-extra');
const path = require('path');
const Promise = require('bluebird');
const sqlite3 = require('sqlite3');
const zlib = require('zlib');
const unzipper = require('unzipper');
const fs = require('fs');
const isTile = require('./isTile');
const isGzipped = require('./isGzipped');

const defaultValue = Cesium.defaultValue;
const defined = Cesium.defined;
const DeveloperError = Cesium.DeveloperError;

module.exports = tilesetToDatabase;

/**
 * Generates a sqlite database for a tileset, saved as a .3dtiles file.
 *
 * @param {String} inputZipFile The input .zip of the tileset.
 * @param {String} [outputFile] The output .3dtiles database file.
 * @returns {Promise} A promise that resolves when the database is written.
 */
function tilesetToDatabase(inputZipFile, outputFile) {
    const start = new Date();
    if (!defined(inputZipFile)) {
        throw new DeveloperError('inputZipFile is required.');
    }

    outputFile = defaultValue(
        outputFile,
        path.join(
            path.dirname(inputZipFile),
            path.basename(inputZipFile) + '.3dtiles.db'
        )
    );

    let db;
    let dbRun;
    // Delete the .3dtiles file if it already exists
    return Promise.resolve(fsExtra.remove(outputFile))
        .then(function () {
            // Create the database.
            db = new sqlite3.Database(
                outputFile,
                sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE
            );
            dbRun = Promise.promisify(db.run, { context: db });

            // Disable journaling and create the table.
            return dbRun('PRAGMA journal_mode=off;');
        })
        .then(function () {
            return dbRun('BEGIN');
        })
        .then(function () {
            return dbRun(
                'CREATE TABLE media (key TEXT PRIMARY KEY, content BLOB)'
            );
        })
        .then(function () {
            return fs
                .createReadStream(inputZipFile)
                .pipe(unzipper.Parse())
                .on('entry', async function (entry) {
                    const type = entry.type; // 'Directory' or 'File'
                    const filePath = entry.path;
                    if (type === 'File') {
                        let data = await entry.buffer();
                        if (isTile(filePath) && !isGzipped(data)) {
                            data = zlib.gzipSync(data);
                        }
                        if (path.extname(filePath) === '.json') {
                            // according to the tilesetToDatabase.js code we gzip the json
                            // but not sure if we want to convert to string first or store
                            // the binary
                            data = data.toString('utf8');
                            data = zlib.gzipSync(data);
                        }
                        dbRun('INSERT INTO media VALUES (?, ?)', [
                            filePath,
                            data
                        ]);
                        entry.autodrain();
                    } else {
                        entry.autodrain();
                    }
                })
                .promise();
        })
        .then(function () {
            return dbRun('COMMIT');
        })
        .finally(function () {
            if (defined(db)) {
                db.close();
            }
            console.log(`finished in ${new Date() - start} ms`);
        });
}
