'use strict';


const Cesium = require('cesium');
const Database = require('better-sqlite3');
const bufferToJson = require('./bufferToJson');
const util = require('./utility');
const {ungzip} = require('node-gzip');
const FileType = require('file-type');
const defined = Cesium.defined;

module.exports = {
    getDBReader: getDBReader
};

async function getDBReader(filePath)
{
    
    const db = new Database(util.normalizePath(filePath));

    if (!db) {
        console.log('Cant open DB');
    }

    const readData = async (path) => {
            const normalizedPath = util.normalizePath(path);
            const stmt = db.prepare('SELECT content FROM media WHERE key = ?');
            const result = stmt.get(normalizedPath);
            return result;
    };

    return {
        readBinary: readData,
        readJson: async (path) => {
            const buffer = await readData(path);
            const ctype = await FileType.fromBuffer(buffer.content);
            if (ctype.mime=='application/gzip')
            {
                const decoded = await (await ungzip(buffer.content)).toString();
                return bufferToJson(decoded);
            } 
            else {
                 return bufferToJson(buffer);
            }
        }
    };
}
