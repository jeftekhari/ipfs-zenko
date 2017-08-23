'use strict';

const ipfsAPI = require('ipfs-api');
const arsenal = require('arsenal');
const werelogs = require('werelogs');
const errors = require('arsenal/errors');

const SUBLEVEL_SEP = '::';

const logOptions = {
    "logLevel": "debug",
    "dumpLevel": "error"
};

const logger = new werelogs.Logger('Zenko-IPFS');

// Metadata

const MetadataFileServer = require('arsenal').storage.metadata.MetadataFileServer;

const mdServer = new MetadataFileServer({
    bindAddress: '0.0.0.0',
    port: 9990,
    path: '/tmp',
    restEnabled: false,
    restPort: 9999,
    recordLog: { enabled: false, recordLogName: 's3-recordlog' },
    versioning: { replicationGroupId: 'RG001' },
    log: logOptions
});

var ipfs = ipfsAPI('/ip4/127.0.0.1/tcp/5001');

class IPFSService extends arsenal.network.rpc.BaseService {
    constructor(params) {
        super(params);
        this.addRequestInfoConsumer((dbService, reqParams) => {
            const env = {};
            env.subLevel = reqParams.subLevel;
            return env;
        });
    }
}

mdServer.initMetadataService = function() {
    const dbService = new IPFSService({
        namespace: '/MDFile/metadata',
        logger: logger
    });
    this.services.push(dbService);

    dbService.registerAsyncAPI({
        put: (env, key, value, options, cb) => {
            const dbName = env.subLevel.join(SUBLEVEL_SEP);
            console.log('put',env,dbName,key,value,options);
        },
        del: (env, key, options, cb) => {
            console.log('del',env,key,options);
        },
        get: (env, key, options, cb) => {
            console.log('get',key,options);
        },
        getDiskUsage: (env, cb) => {
            console.log('getDiskUsage',env);
        },
    });

    dbService.registerSyncAPI({
        createReadStream:
        (env, options) => {
            console.log('createReadStream');
        },
        getUUID: () => this.readUUID(),
    });
    
    console.log('Hooks installed');
};

mdServer.startServer();

// data

class IPFSFileStore extends arsenal.storage.data.file.DataFileStore {
    constructor(dataConfig, logApi) {
        super(dataConfig, logApi);
        console.log('filestore constructor');
    }

    setup(callback) {
        console.log('data setup');
        callback(null);
    }

    put(dataStream, size, log, callback) {
        console.log('data put');
        const files = [
            {
                path: '/tmp/myfile',
                content: dataStream
            }
        ];

        ipfs.files.add(files, function (err, files) {
            if (err) {
                log.error('error putting files',
                          { method: 'put', key, error: err });
                return callback(errors.InternalError.customizeDescription(
                    `filesystem error: open() returned ${err.code}`));
            }
        });
    }

    stat(key, log, callback) {
        console.log('data stat');
    }

    get(key, byteRange, log, callback) {
        console.log('data get');

        const filePath = this.getFilePath(key);
        const cbOnce = jsutil.once(callback);
        ipfs.files.get(key, function (err, stream) {
            stream.on('data', (file) => {
            // write the file's path and contents to standard out
                console.log(file.path)
                file.content.pipe(process.stdout)
            })
        })
    }

    delete(key, log, callback) {
        console.log('data delete');
    }

    getDiskUsage(callback) {
        console.log('data getDiskUsage');
    }
}

const dataServer = new arsenal.network.rest.RESTServer({
    bindAddress: '0.0.0.0',
    port: 9991,
    dataStore: new IPFSFileStore({ 
        dataPath: '/tmp',
        log: logOptions
    }),
    log: logOptions
});

dataServer.setup(err => {
    if (err) {
        logger.error('Error initializing REST data server', { error: err });
        return;
    }

    dataServer.start();
});

console.log('Zenko IPFS Plugin Initialized');
