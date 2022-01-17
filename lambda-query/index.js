// dependencies
const AWS = require('aws-sdk');
const util = require('util');
const timeseries = require("./timeseries-queries");

// Modeled after:
// https://docs.aws.amazon.com/lambda/latest/dg/with-s3-tutorial.html


// get reference to S3 client
const s3 = new AWS.S3();

exports.handler = async (event, context, callback) => {

    // Setup timestream
    /**
    * Recommended Timestream write client SDK configuration:
    *  - Set SDK retry count to 10.
    *  - Use SDK DEFAULT_BACKOFF_STRATEGY
    *  - Set RequestTimeout to 20 seconds .
    *  - Set max connections to 5000 or higher.
    */
    var https = require('https');
    var agent = new https.Agent({
        maxSockets: 5000
    });
    // Configuring AWS SDK
    AWS.config.update({ region: "us-east-1" });

    queryClient = new AWS.TimestreamQuery();

    var results = {}
    results = await timeseries.getLatestWeather(queryClient);

    // testing for now
    let responseCode = 200;
    responseBody = results
    /*
    responseBody = {
        temp: {
            value: 24.0,
            timestamp: 1642429005
        },
        rh: {
            value: 76,
            timestamp: 1642429005
        },
        wind: {
            value: 11.0,
            timestamp: 1642429005
        },
        rain: {
            value: 4.0,
            timestamp: 1642429005
        }

    }
    */

    // The output from a Lambda proxy integration must be 
    // in the following JSON object. The 'headers' property 
    // is for custom response headers in addition to standard 
    // ones. The 'body' property  must be a JSON string. For 
    // base64-encoded payload, you must also set the 'isBase64Encoded'
    // property to 'true'.
    let response = {
        statusCode: responseCode,
        headers: {
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
        },
        body: JSON.stringify(responseBody)
    };
    console.log("response: " + JSON.stringify(response))
    return response;

};
