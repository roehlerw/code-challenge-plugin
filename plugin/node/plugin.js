/**
 * Constants for server 
 */
var PROTO_PATH = __dirname + '/../../plugin.proto';
var PORT = 5000;

/**
 * Dynamically load plugin proto contract
 */
var grpc = require('grpc');
var protoLoader = require('@grpc/proto-loader');
var packageDefinition = protoLoader.loadSync(
    PROTO_PATH,
    {
        keepCase: true,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true
    });
var plugin_proto = grpc.loadPackageDefinition(packageDefinition).plugin;

/**
 * Load libraries
 */
const glob = require('glob');
const utils = require(__dirname + '/utils.js');

/**
 * Exit on SIGINT or SIGKILL
 */
process.on('SIGINT', function () {
    console.log('SIGINT exiting');
    process.exit(0);
});
process.on('SIGKILL', function () {
    console.log('SIGKILL exiting');
    process.exit(0);
});


/**
 * Implements the Discover RPC method.
 */
function Discover(call, callback) {
    console.log('Discover called');

    var filepath = call.request.settings.fileGlob;
    var schemas = [];

    console.log(`Finding files for: ${filepath}`);

    // Get file(s) from request
    glob(filepath, {}, (error, files) => {
        console.log('Files found: ')
        console.log(files);

        // get schema for each file found
        for (var i in files) {
            schemas.push(utils.getSchema(files[i]))
        }

        // wait for all files to finish and return
        Promise.all(schemas).then((values) => {
            var output = [];

            // remove duplicate schemas and put files together into common schemas
            for (var i in values) {
                if (output.length == 0) {
                    output.push(values[i])
                }
                else {
                    var found = false;
                    for (var j in output) {
                        if (utils.areEqualArray(output[j].properties, values[i].properties)) {
                            output[j].settings.push(values[i].settings[0]);
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        output.push(values[i]);
                    }
                }
            }

            console.log('Found schemas');

            callback(null, { schemas: output });
        });
    });
}

/**
 * Implements the Publish RPC method.
 */
function Publish(call, callback) {
    console.log('Publish called');

    // Get file(s) to publish from schema
    var schema = call.request.schema;
    var files = schema.settings.split(',');
    var promises = [];

    console.log('Files found: ')
    console.log(files);

    for (var i in files) {
        promises.push(utils.publishFile(files[i], call));
    }

    // wait for all files to finish and return
    Promise.all(promises).then(() => {
        console.log('Finished publishing');

        call.end();
    });

}

/**
 * Starts an RPC server that receives requests for the Plugin service at the
 * defined server port
 */
function main() {
    // Start server on port PORT
    var server = new grpc.Server();
    server.addService(plugin_proto.Plugin.service, {
        Discover: Discover,
        Publish: Publish
    });
    server.bind('0.0.0.0:' + PORT, grpc.ServerCredentials.createInsecure());
    server.start();

    // Log PORT to console as required
    console.log(PORT);
}

main();