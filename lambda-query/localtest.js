
const AWS = require('aws-sdk');
const timeseries = require("./timeseries-queries");
// Configuring AWS SDK
AWS.config.update({ region: "us-east-1" });
async function callServices() {


    queryClient = new AWS.TimestreamQuery();

    var results = {}
    results = await timeseries.getLatestWeather(queryClient);
    
    console.log(results)

}

callServices();