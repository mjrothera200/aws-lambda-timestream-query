
const AWS = require('aws-sdk');
const timeseries = require("./timeseries-queries");
// Configuring AWS SDK
AWS.config.update({ region: "us-east-1" });
async function callServices() {


    queryClient = new AWS.TimestreamQuery();

    var results = {}
    results = await timeseries.getLatestWeather(queryClient);
    rainresults = await timeseries.getRainFall24(queryClient)
    
    // Add the rain results
    results['rainfall'] = rainresults['rainfall']

    console.log(results)

}

callServices();