/**
 * Load libraries
 */
const csv = require('csvtojson');
const path = require('path');

/**
 * Global variable for discovered schemas
 */
var knownSchemas = [];

/**
 * Dynamically determine type of the input property
 * Type of the property; can be "string", "integer", "number", "datetime", "boolean"
 */
var getType = function getType(property) {
    // try number
    if (!isNaN(Number(property))) {
        if (property.indexOf('.') == -1) {
            return 'integer';
        }
        else {
            return 'number';
        }
    }
    // try boolean
    else if (property.toLowerCase() == 'true' || property.toLowerCase() == 'false') {
        return 'boolean';
    }
    // try datetime
    else if (!isNaN(Date.parse(property))) {
        return 'datetime';
    }
    // else string
    else {
        return 'string';
    }
}
module.exports.getType = getType;

/**
 * Converts input data string to correct data type
 */
var convertToType = function convertToType(data, type) {
    switch (type) {
        case 'integer':
            return parseInt(data);
        case 'number':
            return parseFloat(data);
        case 'boolean':
            return (data.toLowerCase() == 'true' || data.toLowerCase() == 'false');
        case 'datetime':
            var output = new Date(data);
            output.setUTCHours(0, 0, 0, 0);
            return output;
        case 'string':
            return data;
    }
}
module.exports.convertToType = convertToType;

/**
 * Dynamically determine if input is of correct type
 * Type of the property; can be "string", "integer", "number", "datetime", "boolean"
 */
var checkValidTypes = function checkValidTypes(properties, types) {
    for (var i = 0; i < properties.length; i++) {
        var type = getType(properties[i]);
        if (type !== types[i].type) {
            return {
                error: `Expected type: ${types[i].type} but got type: ${type}`,
                index: i,
                invalid: true
            }
        }
    }

    return {
        error: null,
        index: null,
        invalid: false
    }
}
module.exports.checkValidTypes = checkValidTypes;

/**
 * Gets schema for given filepath
 */
var getSchema = function getSchema(filepath) {
    return new Promise((resolve, reject) => {
        console.log(`Getting schema for: ${filepath}`);

        csv()
            .fromFile(filepath)
            .then((data) => {
                var row = data[0];
                var keys = Object.keys(row);
                var values = Object.values(row);
                var properties = [];

                for (var i in keys) {
                    properties.push({
                        name: keys[i],
                        type: getType(values[i])
                    });
                }

                var schema = {
                    name: path.basename(filepath.split('.')[0], '.csv'),
                    settings: [filepath],
                    properties: properties
                };

                var output = checkKnownSchemas(schema, keys);

                console.log(`Schema found for: ${filepath}`)
                resolve(output);
            });
    });
}
module.exports.getSchema = getSchema;

/**
 * Publishes file for given filepath
 */
var publishFile = function publishFile(filepath, call) {
    return new Promise((resolve, reject) => {
        var schema = call.request.schema;
        var types = getPropertyTypes(schema.properties);

        csv({
            output: 'csv'
        })
            .fromFile(filepath)
            .then((data) => {
                for (var i in data) {
                    var row = data[i];
                    var validCheck = checkValidTypes(row, schema.properties);

                    for (var j in row) {
                        if (j != validCheck.index) {
                            row[j] = convertToType(row[j], types[j]);
                        }
                    }

                    if (validCheck.invalid) {
                        row[validCheck.index] = null;
                        call.write({
                            invalid: true,
                            error: validCheck.error,
                            data: JSON.stringify(row)
                        });
                    }
                    else {
                        call.write({
                            invalid: false,
                            error: null,
                            data: JSON.stringify(row)
                        });
                    }
                }

                resolve();
            });
    });
}
module.exports.publishFile = publishFile;

/**
 * Compares 2 arrays for equality
 */
var areEqualArray = function areEqualArray(a, b) {
    return JSON.stringify(a) === JSON.stringify(b);
}
module.exports.areEqualArray = areEqualArray;

/**
 * Checks if given schema matches a known schema
 * Mutates and returns input schema to known if found
 * Appends schema to known schemas if not found
 */
var checkKnownSchemas = function checkKnownSchemas(schema, header) {
    var output = schema;
    var found = false;

    for (var i in knownSchemas) {
        var properties = knownSchemas[i].properties;

        if (properties.length == header.length) {
            var knownHeader = getPropertyNames(properties);

            if (areEqualArray(knownHeader, header)) {
                console.log(`Known schema found: ${knownSchemas[i].name}`);
                output.name = knownSchemas[i].name;
                output.properties = knownSchemas[i].properties;
                found = true;
                break;
            }
        }
    }
    if (!found) {
        console.log(`New schema found: ${schema.name}`);
        knownSchemas.push(schema);
    }

    return output;
}
module.exports.checkKnownSchemas = checkKnownSchemas;

/**
 * Converts properties object into a header name array
 */
var getPropertyNames = function getPropertyNames(properties) {
    var output = [];

    for (var i in properties) {
        output.push(properties[i].name);
    }

    return output;
}
module.exports.getPropertyNames = getPropertyNames;

/**
 * Converts properties object into a header type array
 */
var getPropertyTypes = function getPropertyTypes(properties) {
    var output = [];

    for (var i in properties) {
        output.push(properties[i].type);
    }

    return output;
}
module.exports.getPropertyTypes = getPropertyTypes;