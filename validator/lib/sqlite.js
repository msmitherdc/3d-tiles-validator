'use strict';


const Cesium = require('cesium');
const path = require('path');
const Database = require('sqlite3');
const bufferToJson = require('./bufferToJson');
const defined = Cesium.defined;

module.exports = {
    getDBReader: getDBReader
};


async function getDBReader(filePath)
{
    const dbPromise = new Promise(
            (resolve, reject) => {
                //const db = new Database('foobar.db', { verbose: console.log });
                const db = new Database(filePath);
                db.on('error', (err) => {
                    //console.log(`Error: ${err}`);
                    reject(err);
                });
                db.on('ready', () => {
                    resolve(db);
                });
            }
        )
        .catch(() => { console.error(`Failed to read ${path.basename(filePath)} as sqlite archive`); });

    const db = await dbPromise;
    if (!db) {
        throw Error();
    }

    return {
        readBinary: (path) => {
            const stmt = db.prepare('SELECT content FROM media WHERE key = ?');
            result = stmt.get(path);
            return result;
        },
        readJson: (path) => {
            const stmt = db.prepare('SELECT content FROM media WHERE key = ?');
            result = stmt.get(path);
            return bufferToJson(result);
        }
    };
}

