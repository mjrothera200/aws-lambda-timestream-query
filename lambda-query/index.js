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

    if (event.path === '/latest') {
        results = await timeseries.getLatestWeather(queryClient);
        rainresults = await timeseries.getRainFall24(queryClient)
        temploggerresults = await timeseries.getLatestTempLogger(queryClient)
        waterqualityresults = await timeseries.getLatestWaterQuality(queryClient)

        // Add the rain results
        results['rainfall'] = rainresults['rainfall']
        // Add the temp logger results
        results['watertemp'] = temploggerresults['tempf']
        results['waterlight'] = temploggerresults['lumensft2']
        // Add the water quality results
        results['watertemprt'] = waterqualityresults['tempf']
        results['salinity'] = waterqualityresults['salinity']
        results['tds'] = waterqualityresults['tds']
        results['ec'] = waterqualityresults['ec']
    } else if (event.path === '/historical') {
        const measure = event.queryStringParameters.measure
        const timeframe = event.queryStringParameters.timeframe
        results = await timeseries.getHistorical(queryClient, measure, timeframe)
        results_events = await timeseries.getEvents(queryClient, measure, timeframe)
        results["events"] = results_events.dataset
        // Fix the events data set to put the "max" on the y for each event to ensure
        var i = 0
        while (i < results["events"].length) {

            // Search for the nearest X in the overall data set, then take the "y" and apply an offset
            var j = 0
            var found_y = results["hints"][0].max.y // Default to the top if not found
            while (j < results["dataset"].length) {
                if (results["dataset"][j].x > results["events"][i].x) {
                    found_y = results["dataset"][j].y
                    break;
                }
                j = j + 1
            }

            results["events"][i].y = found_y
            i = i + 1
        }

    } else if (event.path === '/summary') {
        const measure = event.queryStringParameters.measure
        const year = event.queryStringParameters.year
        results = await timeseries.getHistoricalSummary(queryClient, measure, year)
    } else if (event.path === '/measures') {
        results = await timeseries.getMeasures()
    }

    // testing for now
    let responseCode = 200;
    responseBody = results

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
