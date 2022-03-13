
const AWS = require('aws-sdk');
const timeseries = require("./timeseries-queries");
// Configuring AWS SDK
AWS.config.update({ region: "us-east-1" });
async function callServices() {


    queryClient = new AWS.TimestreamQuery();

    
    var results = {}

    /*
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
 */

    //var results = {}
    results = await timeseries.getHistorical(queryClient, "ec", "3")
    results_events = await timeseries.getEvents(queryClient, "ec", "3")
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

    //results = await timeseries.getMeasures()


    //results = await timeseries.getHistoricalSummary(queryClient, "watertemp", 2022)



    console.log(results)

}

callServices();