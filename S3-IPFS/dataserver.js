'use strict'; // eslint-disable-line strict

const arsenal = require('arsenal');
const { config } = require('./lib/Config.js');
const logger = require('./lib/utilities/logger');

const fs = require('fs');
const crypto = require('crypto');
const async = require('async');
const diskusage = require('diskusage');
const werelogs = require('werelogs');

const errors = require('arsenal').errors;
const stringHash = require('arsenal').stringHash;
const jsutil = require('arsenal').jsutil;
const storageUtils = require('arsenal').storage.utils;

const ipfsAPI = require('ipfs-api');

const ipfs = ipfsAPI('/ip4/127.0.0.1/tcp/5001');

class IPFSFileStore extends arsenal.storage.data.file.DataFileStore {
    put(dataStream, size, log, callback) {
        const key = crypto.pseudoRandomBytes(20).toString('hex');
        const filePath = this.getFilePath(key);

        const files = [
            {
                path: 'file',
                content: dataStream
            }
        ];

        ipfs.files.add(files, function (err, files) {
          // 'files' will be an array of objects
        });

        log.debug('starting to write data', { method: 'put', key, filePath });
        dataStream.pause();
        fs.open(filePath, 'wx', (err, fd) => {
            if (err) {
                log.error('error opening filePath',
                          { method: 'put', key, filePath, error: err });
                return callback(errors.InternalError.customizeDescription(
                    `filesystem error: open() returned ${err.code}`));
            }
            const cbOnce = jsutil.once(callback);
            // disable autoClose so that we can close(fd) only after
            // fsync() has been called
            const fileStream = fs.createWriteStream(filePath,
                                                    { fd,
                                                      autoClose: false });

            fileStream.on('finish', () => {
                function ok() {
                    log.debug('finished writing data',
                              { method: 'put', key, filePath });
                    return cbOnce(null, key);
                }
                if (this.noSync) {
                    fs.close(fd);
                    return ok();
                }
                fs.fsync(fd, err => {
                    fs.close(fd);
                    if (err) {
                        log.error('fsync error',
                                  { method: 'put', key, filePath,
                                    error: err });
                        return cbOnce(
                            errors.InternalError.customizeDescription(
                                'filesystem error: fsync() returned ' +
                                    `${err.code}`));
                    }
                    return ok();
                });
                return undefined;
            }).on('error', err => {
                log.error('error streaming data on write',
                          { method: 'put', key, filePath, error: err });
                // destroying the write stream forces a close(fd)
                fileStream.destroy();
                return cbOnce(errors.InternalError.customizeDescription(
                    `write stream error: ${err.code}`));
            });
            dataStream.resume();
            dataStream.pipe(fileStream);
            dataStream.on('error', err => {
                log.error('error streaming data on read',
                    { method: 'put', key, filePath, error: err });
                // destroying the write stream forces a close(fd)
                fileStream.destroy();
                return cbOnce(errors.InternalError.customizeDescription(
                    `read stream error: ${err.code}`));
            });
            return undefined;
        });
    }

    get(key, byteRange, log, callback) {
        const filePath = this.getFilePath(key);

        const readStreamOptions = {
            flags: 'r',
            encoding: null,
            fd: null,
            autoClose: true,
        };
        if (byteRange) {
            readStreamOptions.start = byteRange[0];
            readStreamOptions.end = byteRange[1];
        }
        log.debug('opening readStream to get data',
                  { method: 'get', key, filePath, byteRange });
        const cbOnce = jsutil.once(callback);
        const rs = fs.createReadStream(filePath, readStreamOptions)
                  .on('error', err => {
                      if (err.code === 'ENOENT') {
                          return cbOnce(errors.ObjNotFound);
                      }
                      log.error('error retrieving file',
                                { method: 'get', key, filePath,
                                  error: err });
                      return cbOnce(
                          errors.InternalError.customizeDescription(
                              `filesystem read error: ${err.code}`));
                  })
                  .on('open', () => { cbOnce(null, rs); });

        ipfs.files.get(key, function (err, stream) {
            stream.on('data', (file) => {
            // write the file's path and contents to standard out
                console.log(file.path);
                file.content.pipe(process.stdout);
                cbOnce(null, stream);
            });
        });
    }

    stat(key, log, callback) {

        ipfs.object.stat(key, (err, stats) => {
            if (err) {
                throw err
            }
            console.log(stats)
            // Logs:
            // {
            //   Hash: 'QmPTkMuuL6PD8L2SwTwbcs1NPg14U8mRzerB1ZrrBrkSDD',
            //   NumLinks: 0,
            //   BlockSize: 10,
            //   LinksSize: 2,
            //   DataSize: 8,
            //   CumulativeSize: 10
            // }
        });

        const filePath = this.getFilePath(key);
        log.debug('stat file', { key, filePath });
        fs.stat(filePath, (err, stat) => {
            if (err) {
                if (err.code === 'ENOENT') {
                    return callback(errors.ObjNotFound);
                }
                log.error('error on \'stat\' of file',
                          { key, filePath, error: err });
                return callback(errors.InternalError.customizeDescription(
                    `filesystem error: stat() returned ${err.code}`));
            }
            const info = { objectSize: stat.size };
            return callback(null, info);
        });
    }
}

if (config.backends.data === 'file' ||
    (config.backends.data === 'multiple' &&
     config.backends.metadata !== 'scality')) {
    const dataServer = new arsenal.network.rest.RESTServer(
        { bindAddress: config.dataDaemon.bindAddress,
          port: config.dataDaemon.port,
          dataStore: new IPFSFileStore({ 
            dataPath: '/tmp',
            log: config.log
          }),
          log: config.log });
    dataServer.setup(err => {
        if (err) {
            logger.error('Error initializing REST data server',
                         { error: err });
            return;
        }
        dataServer.start();
    });
}
