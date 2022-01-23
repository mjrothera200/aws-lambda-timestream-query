
const AWS = require('aws-sdk');
const timeseries = require("./timeseries-queries");
// Configuring AWS SDK
AWS.config.update({ region: "us-east-1" });
async function callServices() {


    queryClient = new AWS.TimestreamQuery();

    /*
    var results = {}
    results = await timeseries.getLatestWeather(queryClient);
    rainresults = await timeseries.getRainFall24(queryClient)
    temploggerresults = await timeseries.getLatestTempLogger(queryClient)
    // Add the rain results
    results['rainfall'] = rainresults['rainfall']

    // Add the temp logger results
    results['watertemp'] = temploggerresults['tempf']
    results['waterlight'] = temploggerresults['lumensft2']
    */


    var results = {}
    results = await timeseries.getHistorical(queryClient, "tempf", "all")

    console.log(results)

}

callServices();