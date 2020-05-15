'use strict';
const Cesium = require('cesium');
const fsExtra = require('fs-extra');
const path = require('path');
const StreamZip = require('node-stream-zip');
const crypto = require('crypto');
const bufferToJson = require('./bufferToJson');
const util = require('./utility');
const defined = Cesium.defined;

module.exports = {
    readIndex: readIndex,
    listIndex: listIndex,
    validateIndex: validateIndex,
    getIndexReader: getIndexReader,
    getZipReader: getZipReader
};

const ZIP_END_OF_CENTRAL_DIRECTORY_HEADER_SIG = 0x06054b50;
const ZIP_START_OF_CENTRAL_DIRECTORY_HEADER_SIG = 0x02014b50;
const ZIP64_EXTENDED_INFORMATION_EXTRA_SIG = 0x0001;
const ZIP_LOCAL_FILE_HEADER_STATIC_SIZE = 30;
const ZIP_CENTRAL_DIRECTORY_STATIC_SIZE = 46;

function getLastCentralDirectoryEntry(fd, stat) {
    const bytesToRead = 320;
    let buffer = Buffer.alloc(bytesToRead);
    let offset = stat.size - bytesToRead;
    let length = bytesToRead;
    return fsExtra.read(fd, buffer, 0, length, offset)
        .then(obj => {
        let start = 0, end = 0;
        for (let i = obj.buffer.length - 4; i > 0; i--) {
            let val = obj.buffer.readUInt32LE(i);
            if (val == ZIP_END_OF_CENTRAL_DIRECTORY_HEADER_SIG) {
                end = i;
            }
            if (val == ZIP_START_OF_CENTRAL_DIRECTORY_HEADER_SIG) {
                start = i;
                break;
            }
        }

        if (start != end) {
            return obj.buffer.slice(start);
        }

        return obj.buffer;
    });
}

async function validateCentralDirectoryHeaderAndGetFileContents(fd, buffer, expectedFilename)
{
    let header = {};
    header.signature = buffer.readUInt32LE(0);
    if (header.signature != ZIP_START_OF_CENTRAL_DIRECTORY_HEADER_SIG) {
        throw Error(`Bad central directory file header signature: ${header.signature}`);
    }
    header.version_madeby = buffer.readUInt16LE(4);
    header.version_needed = buffer.readUInt16LE(6);
    header.bitflags = buffer.readUInt16LE(8);

    const disallowed_flags_mask =
        (1 << 0) |  // File must not be encrypted
        (1 << 3) |  // Local File Headers must have file sizes set
        (1 << 5) |  // No compressed patched data
        (1 << 13);  // No encrypted central directory

    if (disallowed_flags_mask & header.bitflags) {
        throw Error(`Zip has disallowed bitflags set in Central Directory Header: 0b${(disallowed_flags_mask & header.bitflags).toString(2)}`);
    }

    header.comp_method = buffer.readUInt16LE(10);
    if (header.comp_method !== 0) {
        throw Error(`Zip must use STORE compression method, found compression method ${header.comp_method}`);
    }
    header.last_mod_time = buffer.readUInt16LE(12);
    header.last_mod_date = buffer.readUInt16LE(14);
    header.crc = buffer.readUInt32LE(16);
    header.comp_size = buffer.readUInt32LE(20);
    header.uncomp_size = buffer.readUInt32LE(24);
    header.filename_size = buffer.readUInt16LE(28);
    header.extra_size = buffer.readUInt16LE(30);
    header.comment_size = buffer.readUInt16LE(32);
    header.disk_number = buffer.readUInt16LE(34);
    header.int_attrib = buffer.readUInt16LE(36);
    header.ext_attrib = buffer.readUInt32LE(38);

    let filename = buffer.toString('utf8', ZIP_CENTRAL_DIRECTORY_STATIC_SIZE, ZIP_CENTRAL_DIRECTORY_STATIC_SIZE + header.filename_size);    
    if (filename !== expectedFilename)
    {
        throw Error(`Central Directory File Header filename was ${filename}, expected ${expectedFilename}`);
    }

    header.offset = buffer.readUInt32LE(42);
    // if we get this offset, then the offset is stored in the 64 bit extra field
    if (header.offset == 0xFFFFFFFF) {
        let offset64Found = false;
        let endExtrasOffset = ZIP_CENTRAL_DIRECTORY_STATIC_SIZE + header.filename_size + header.extra_size;
        let currentOffset = ZIP_CENTRAL_DIRECTORY_STATIC_SIZE + header.filename_size;
        while (!offset64Found && currentOffset < endExtrasOffset) {
          let extra_tag = buffer.readUInt16LE(currentOffset);
          let extra_size = buffer.readUInt16LE(currentOffset + 2);
          if (extra_tag == ZIP64_EXTENDED_INFORMATION_EXTRA_SIG && extra_size == 8) {
            header.offset = buffer.readBigUInt64LE(currentOffset + 4);
            offset64Found = true;
            //console.log(`found 64bit relative offset: ${header.offset}`);
          }
          else {
            currentOffset += extra_size;
          }
        }
        if (!offset64Found) {
          throw Error("No zip64 extended offset found");
        }
    }

    const localFileHeaderSize = ZIP_LOCAL_FILE_HEADER_STATIC_SIZE + header.filename_size +
        + 48 /* over-estimated local file header extra field size, to try and read all data in one go */
        + header.comp_size;
    let localFileHeaderBuffer = Buffer.alloc(localFileHeaderSize);

    //console.log(`Reading local file header from offset: ${header.offset}`);
    return fsExtra.read(fd, localFileHeaderBuffer, 0, localFileHeaderSize, Number(header.offset))
        .then(obj => obj.buffer)
        .catch(err => console.log(`Got error: ${err}`));
}

function parseLocalFileHeaderAndValidateFilename(buffer, expectedFilename)
{
    let header = {};
    header.signature = buffer.readUInt32LE(0);
    if (header.signature != 0x04034b50) {
        throw Error(`Bad local file header: ${header.signature}`);
    }

    header.version_needed = buffer.readUInt16LE(4);
    header.general_bits = buffer.readUInt16LE(6);
    header.compression_method = buffer.readUInt16LE(8);
    header.last_mod_time = buffer.readUInt16LE(10);
    header.last_mod_date = buffer.readUInt16LE(12);
    header.crc32 = buffer.readUInt32LE(14);
    header.comp_size = buffer.readUInt32LE(18);
    header.uncomp_size = buffer.readUInt32LE(22);
    header.filename_size = buffer.readUInt16LE(26);
    header.extra_size = buffer.readUInt16LE(28);

    let filename = buffer.toString('utf8', ZIP_LOCAL_FILE_HEADER_STATIC_SIZE, ZIP_LOCAL_FILE_HEADER_STATIC_SIZE + header.filename_size);
    if (filename !== expectedFilename)
    {
        throw Error(`Local File Header filename was ${filename}, expected ${expectedFilename}`);
    }

    let compressedSize = header.comp_size;
    if (compressedSize === 0) {
        throw Error(`Zip Local File Headers must have non-zero file sizes set.`);
    }
    return header;
}

function md5LessThan(md5hashA, md5hashB) {
    const aLo = md5hashA.readBigUInt64LE();
    const bLo = md5hashB.readBigUInt64LE();
    if (aLo == bLo) {
        const aHi = md5hashA.readBigUInt64LE(8);
        const bHi = md5hashB.readBigUInt64LE(8);
        return aHi < bHi;
    }
    return aLo < bLo;
}

function md5comp(a, b) {
    return md5LessThan(a.md5hash, b.md5hash) ? -1 : 1;
}

function md5AsUInt64(md5hashBuffer) {
    return [md5hashBuffer.readBigUInt64LE(0), md5hashBuffer.readBigUInt64LE(8)];
}

function zipIndexFind(zipIndex, searchHash) {
    let low = 0;
    let high = zipIndex.length - 1;
    while(low <= high) {
        let mid = Math.floor(low + (high - low) / 2);
        const entry = zipIndex[mid];
        //console.log(`mid: ${mid} entry: ${entry.md5hash.toString('hex')}`);
        if(entry.md5hash.compare(searchHash) === 0)
            return mid;
        else if (md5LessThan(zipIndex[mid].md5hash, searchHash))
            low = mid + 1;
        else
            high = mid - 1;
    }

    return -1;
}

async function listIndex(zipIndex, range) {
    let start = 0;
    let end = -1;
    if (range.length === 1) {
        start = range[0];
    } else if (range.length === 2){
        [start, end] = range;
    } else {
        console.error(`Invalid range, ${range}`);
        return;
    }
    if (start < 0) {
        console.error(`Range start must be positive, ${start}`);
        return;
    }
    if (end < 0 || end > zipIndex.length) {
        end = zipIndex.length;
    }
    for (let i = start; i < end; i++) {
        const entry = zipIndex[i];
        const [hashHi,hashLo] = md5AsUInt64(entry.md5hash);
        console.log(`${i}: ${hashHi} ${hashLo} ${entry.md5hash.toString('hex')} offset: ${entry.offset}`);
    }
    return;
}

function slowValidateIndex(zipIndex, zipFilePath) {
    console.time("thorough validate index");
    let valid = true;
    const zip = new StreamZip({
        file: zipFilePath,
        storeEntries: false
    });
    try {
        zip.on('error', err => { throw Error(err) });
        zip.on('ready', () => {
            console.log('Entries read: ' + zip.entriesCount);
            zip.close();
            console.log("Closed zip");
        });
        zip.on('entry', entry => {
            if (entry.isFile && entry.name !== "@3dtilesIndex1@") {
                //console.log(`Validating index entry for ${entry.name}`);
                let hash = crypto.createHash('md5').update(entry.name).digest();
                let index = zipIndexFind(zipIndex, hash);
                if (index === -1) {
                    throw Error(`${entry.name} - ${hash} not found in index.`);
                } else {
                    const indexEntryOffset = zipIndex[index].offset;
                    if (entry.offset != indexEntryOffset) {
                        throw Error(`${entry.name} - ${hash} had incorrect offset ${indexEntryOffset}, expected ${entry.offset}`)
                    }
                }
            }
        });
    }
    catch (err) {
        valid = false;
    }

    console.timeEnd("thorough validate index");
    return valid;
}

async function validateIndex(zipIndex, zipFilePath, quick) {
    console.time("validate index");
    let valid = true;
    const numItems = zipIndex.length;
    if (numItems > 1) {
        let errors = {
            collisions: []
        };
        for (let i = 1; i < numItems; i++) {
            const prevEntry = zipIndex[i-1];
            const curEntry = zipIndex[i];
            const [curHashHi, curHashLo] = md5AsUInt64(curEntry.md5hash);
            if (prevEntry.md5hash.compare(curEntry.md5hash) === 0) {
                errors.collisions.push([i-1, i]);
            }

            const [prevHashHi, prevHashLo] = md5AsUInt64(prevEntry.md5hash);

            if (!md5LessThan(prevEntry.md5hash, curEntry.md5hash)) {
                console.warn(`Wrong sort order\n${i}: ${curEntry.md5hash.toString('hex')} (${curHashHi} ${curHashLo}) should be smaller than\n${i-1}: ${prevEntry.md5hash.toString('hex')} (${prevHashHi} ${prevHashLo})`);
                valid = false;
            }
        }

        if (errors.collisions.length) {
            for (let c of errors.collisions) {
                console.warn(`Got hash collision at index ${c[0]} and ${c[1]}`);
            }
        }
    }

    let rootHash = crypto.createHash('md5').update("tileset.json").digest();
    let rootIndex = zipIndexFind(zipIndex, rootHash);
    if (rootIndex === -1) {
        valid = false;
        console.error(`Index has no key for the root tileset`);
    } else {
        let fd = await fsExtra.open(zipFilePath, "r");
        try {
            await ReadZipLocalFileHeader(fd, zipIndex[rootIndex].offset, "tileset.json");
        }
        catch(err) {
            valid = false;
            console.error(err);
        }
        fsExtra.close(fd);
    }

    if (!quick && valid) {
        valid = slowValidateIndex(zipIndex, zipFilePath);
    }

    console.log(`Index is ${valid ? "valid" : "invalid"}`);
    console.timeEnd("validate index");
    return valid;
}

function parseIndexData(buffer) {
    if (buffer.length % 24 !== 0) {
        console.error(`Bad index buffer length: ${buffer.length}`);
        return -1;
    }
    const numEntries = buffer.length / 24;
    let index = [];
    console.log(`Zip index contains ${numEntries} entries.`);
    for (let i = 0; i < numEntries; i++) {
        let byteOffset = i * 24;
        let hash = buffer.slice(byteOffset, byteOffset + 16);
        let offset = buffer.readBigUInt64LE(byteOffset + 16);
        index.push({"md5hash": hash, "offset": offset});
    }
    return index;
}

// read index from a raw file
async function readIndexData(inputFile) {
    return fsExtra.readFile(inputFile)
        .then(buffer => parseIndexData(buffer));
}

async function searchIndex(zipIndex, searchPath) {
    let hashedSearchPath = crypto.createHash('md5').update(searchPath).digest();
    //console.log(`Searching index for ${searchPath} (${hashedSearchPath.toString('hex')})`);
    
    //console.time('Search index');
    let matchedIndex = zipIndexFind(zipIndex, hashedSearchPath);
    //console.log(`matchedIndex: ${matchedIndex}`);
    //console.timeEnd('Search index');
    if (matchedIndex === -1) {
        console.log(`Couldn't find ${searchPath} (${hashedSearchPath.toString('hex')})`);
        return undefined;
    }

    let entry = zipIndex[matchedIndex];
    //console.log(`Matched index: ${matchedIndex} - offset: ${entry.offset}`);
    return entry;
}

// if outputFile is not supplied, read index into memory
async function readIndex(inputFile, outputFile, indexFilename = "@3dtilesIndex1@") {
    // console.log(`Read index from ${inputFile}`);
    console.time('readIndex');
    let fd;
    return fsExtra.open(inputFile, 'r')
        .then(f => {
            fd = f;
            return fsExtra.fstat(fd);
        })
        .then(stat => {
            return getLastCentralDirectoryEntry(fd, stat);
        })
        .then(buffer => {
            return validateCentralDirectoryHeaderAndGetFileContents(fd, buffer, indexFilename);
        })
        .then(buffer => {
            let header = parseLocalFileHeaderAndValidateFilename(buffer, indexFilename);

            // ok, skip past the filename and extras and we have our data
            let dataStartOffset = ZIP_LOCAL_FILE_HEADER_STATIC_SIZE + header.filename_size + header.extra_size;

            const indexFileDataBuffer = buffer.slice(
                dataStartOffset, dataStartOffset + header.comp_size);
            if (indexFileDataBuffer.length === 0) {
                throw Error(`Failed to get file data at offset ${dataStartOffset}`);
            }

            if (defined(outputFile)) {
                return fsExtra.writeFile(outputFile, indexFileDataBuffer);
            } else {
                return parseIndexData(indexFileDataBuffer);
            }
        })
        .catch(err => {
            console.error(err.message);
            throw err;
        })
        .finally(() => {
            fsExtra.close(fd);
            console.timeEnd('readIndex');
        });
}

async function ReadZipLocalFileHeader(fd, offset, path)
{
    const headerSize = ZIP_LOCAL_FILE_HEADER_STATIC_SIZE + path.length;
    let headerBuffer = Buffer.alloc(headerSize);
    //console.log(`ReadZipLocalFileHeader path: ${path} headerSize: ${headerSize} offset: ${offset}`);
    let result = await fsExtra.read(fd, headerBuffer, 0, headerSize, Number(offset));
    //console.log(`headerBuffer: ${result.buffer}`);
    let header = parseLocalFileHeaderAndValidateFilename(result.buffer, path);
    //console.log(header);
    return header;
}

async function getIndexReader(filePath, performIndexValidation)
{
    const index = await readIndex(filePath);
    if (performIndexValidation) {
        await validateIndex(index, filePath);
    }

    let fd = await fsExtra.open(filePath, 'r');

    let readData = async (path) => {
        let normalizedPath = util.normalizePath(path);
        let match = await searchIndex(index, normalizedPath);
        if (match !== undefined) {
            let header = await ReadZipLocalFileHeader(fd, match.offset, path);
            let fileDataOffset = Number(match.offset) + ZIP_LOCAL_FILE_HEADER_STATIC_SIZE + header.filename_size + header.extra_size;
            let fileContentsBuffer = Buffer.alloc(header.comp_size);
            //console.log(`Fetching data at offset ${fileDataOffset} size: ${header.comp_size}`);
            let data = await fsExtra.read(fd, fileContentsBuffer, 0, header.comp_size, fileDataOffset);
            return data.buffer;
        }
        throw Error(path);
    };

    return {
        readBinary: readData,
        readJson: async (path) => {
            let buffer = await readData(path);
            return bufferToJson(buffer);
        }
    };
}

async function getZipReader(filePath)
{
    let zipPromise = new Promise(
            (resolve, reject) => {
                let streamzip = new StreamZip({
                    file: filePath,
                    storeEntries: true
                });
                streamzip.on('error', (err) => {
                    //console.log(`Error: ${err}`);
                    reject(err);
                })
                streamzip.on('ready', () => {
                    resolve(streamzip);
                });
            }
        )
        .catch(() => { console.error(`Failed to read ${path.basename(filePath)} as zip archive`) });

    let zip = await zipPromise;
    if (!zip) {
        throw Error();
    }

    return {
        readBinary: (path) => {
            return zip.entryDataSync(path);
        },
        readJson: (path) => {
            let buffer = zip.entryDataSync(path);
            return bufferToJson(buffer);
        }
    };
}