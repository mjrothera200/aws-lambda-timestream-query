const constants = require('./constants');

const HOSTNAME = "host1";

// See records ingested into this table so far
const SELECT_ALL_QUERY = "SELECT * FROM \"" + constants.DATABASE_NAME + "\".\"" + constants.TEMP_LOGGER_TABLE_NAME + "\"";

// Note names of fields - important for conversion routine
const SELECT_LATEST_WEATHER = "SELECT measure_name, to_milliseconds(time) AS unixtime, measure_value::varchar as value FROM \"" + constants.DATABASE_NAME + "\".\"" + constants.WEATHER_DATA_TABLE_NAME + "\" WHERE time between ago(15m) and now() ORDER BY time DESC LIMIT 20"

const SELECT_LATEST_TEMPLOGGER = "SELECT measure_name, to_milliseconds(time) AS unixtime, measure_value::double as value FROM \"" + constants.DATABASE_NAME + "\".\"" + constants.TEMP_LOGGER_TABLE_NAME + "\" ORDER BY time DESC LIMIT 2"

const SELECT_RAINFALL24 = "SELECT measure_name, to_milliseconds(time) AS unixtime, measure_value::varchar as value FROM \"" + constants.DATABASE_NAME + "\".\"" + constants.WEATHER_DATA_TABLE_NAME + "\" WHERE measure_name = 'rain' and time between ago(24h) and now() ORDER BY time ASC LIMIT 100"

async function getHistorical(queryClient, measure_name, timeframe) {

    var timeclause = ""
    if (timeframe === "YTD") {
        timeclause = "YEAR(time) = YEAR(now())"
    } else if (timeframe === "MTD") {
        timeclause = "MONTH(time) = MONTH(now())"
    } else {
        timeclause = "DAY(time) = DAY(now())"
    }
    const query = `SELECT to_milliseconds(time) AS x, measure_value::double as y FROM "${constants.DATABASE_NAME}"."${constants.TEMP_LOGGER_TABLE_NAME}" WHERE measure_name = '${measure_name}' and ${timeclause} ORDER BY time ASC`

    const queries = [query];

    var parsedRows = []
    for (let i = 0; i < queries.length; i++) {
        console.log(`Running query ${i + 1} : ${queries[i]}`);
        parsedRows = await getAllRows(queryClient, queries[i], null);
    }
    const results = convertHistoricalRows(parsedRows)
    return results
}

function convertHistoricalRows(parsedRows) {
    var results = {}
    var dataset = []
    var hints = []
    var max = {x: 0, y: 99999}
    var min = {x: 0, y: -99999}
    if (parsedRows) {
        parsedRows.forEach(function (row) {
            const splits = row.split(',')
            var entry = {}
            var sensorname = ""
            splits.forEach(function (field) {
                const fieldSplit = field.split('=')
                const field_name = fieldSplit[0].trim()
                const field_value = fieldSplit[1].trim()
                if (field_name === 'x') {
                    entry.x = parseInt(field_value)
                } else if (field_name === 'y') {
                    entry.y = parseFloat(field_value)
                    
                }
            });
            if (entry.y < max.y) {
                max = entry
            }
            if (entry.y > min.y) {
                min = entry
            }
            dataset.push(entry);
        }
        );
    }
    hints.push({type: "max", max})
    hints.push({type: "min", min})
    results["dataset"] = dataset
    results["hints"] = hints
    return results;
}


async function getRainFall24(queryClient) {
    const queries = [SELECT_RAINFALL24];

    var parsedRows = []
    for (let i = 0; i < queries.length; i++) {
        console.log(`Running query ${i + 1} : ${queries[i]}`);
        parsedRows = await getAllRows(queryClient, queries[i], null);
    }
    const results = aggregateRainFall(parsedRows)
    return results
}

function aggregateRainFall(parsedRows) {
    var results = {}
    var rainfall = 0.0
    var previous_counter = -1
    var finalentry = {}
    var lasttimestamp = -1
    if (parsedRows) {
        parsedRows.forEach(function (row) {
            const splits = row.split(',')
            var sensorname = ""
            var entry = {}
            splits.forEach(function (field) {
                const fieldSplit = field.split('=')
                const field_name = fieldSplit[0].trim()
                const field_value = fieldSplit[1].trim()
                if (field_name === 'measure_name') {
                    sensorname = field_value
                } else if (field_name === 'unixtime') {
                    entry.timestamp = (field_value / 1000).toFixed();

                    lasttimestamp = entry.timestamp

                } else if (field_name === 'value') {
                    entry.value = field_value
                }

            });
            const counter = parseInt(entry.value)
            if (counter > 0) {
                // 
                if (previous_counter > 0) {
                    if (counter > previous_counter) {
                        const difference = counter - previous_counter;
                        rainfall = rainfall + (difference * .01)
                    }
                    previous_counter = counter;
                } else {
                    previous_counter = counter;
                }
            }

        }
        );
    }

    finalentry.value = rainfall.toFixed(2);
    finalentry.timestamp = lasttimestamp
    results['rainfall'] = finalentry;

    return results;
}

async function getLatestTempLogger(queryClient) {
    const queries = [SELECT_LATEST_TEMPLOGGER];

    var parsedRows = []
    for (let i = 0; i < queries.length; i++) {
        console.log(`Running query ${i + 1} : ${queries[i]}`);
        parsedRows = await getAllRows(queryClient, queries[i], null);
    }
    const results = convertRows(parsedRows)
    return results
}

async function getLatestWeather(queryClient) {
    const queries = [SELECT_LATEST_WEATHER];

    var parsedRows = []
    for (let i = 0; i < queries.length; i++) {
        console.log(`Running query ${i + 1} : ${queries[i]}`);
        parsedRows = await getAllRows(queryClient, queries[i], null);
    }
    const results = convertRows(parsedRows)
    return results
}

function convertRows(parsedRows) {
    var results = {}
    if (parsedRows) {
        parsedRows.forEach(function (row) {
            const splits = row.split(',')
            var entry = {}
            var sensorname = ""
            splits.forEach(function (field) {
                const fieldSplit = field.split('=')
                const field_name = fieldSplit[0].trim()
                const field_value = fieldSplit[1].trim()
                if (field_name === 'measure_name') {
                    sensorname = field_value
                } else if (field_name === 'unixtime') {
                    entry.timestamp = (field_value / 1000).toFixed();
                } else if (field_name === 'value') {
                    entry.value = field_value
                }
            });
            results[sensorname] = entry;
        }
        );
    }

    return results;
}


async function getAllRows(queryClient, query, nextToken = undefined) {
    let response;
    try {
        response = await queryClient.query(params = {
            QueryString: query,
            NextToken: nextToken,
        }).promise();
    } catch (err) {
        console.error("Error while querying:", err);
        throw err;
    }

    var parsedRows = parseQueryResult(response);
    if (response.NextToken) {
        parsedRows.concat(await getAllRows(query, response.NextToken));
    }
    return parsedRows
}

function parseQueryResult(response) {
    const queryStatus = response.QueryStatus;
    //console.log("Current query status: " + JSON.stringify(queryStatus));

    const columnInfo = response.ColumnInfo;
    const rows = response.Rows;

    //console.log("Metadata: " + JSON.stringify(columnInfo));
    //console.log("Data: ");

    var parsedQueryRows = []
    rows.forEach(function (row) {
        const entry = parseRow(columnInfo, row)
        parsedQueryRows.push(entry);
    });
    return parsedQueryRows
}

function parseRow(columnInfo, row) {
    const data = row.Data;
    const rowOutput = [];

    var i;
    for (i = 0; i < data.length; i++) {
        info = columnInfo[i];
        datum = data[i];
        rowOutput.push(parseDatum(info, datum));
    }

    return `${rowOutput.join(", ")}`
}

function parseDatum(info, datum) {
    if (datum.NullValue != null && datum.NullValue === true) {
        return `${info.Name}=NULL`;
    }

    const columnType = info.Type;

    // If the column is of TimeSeries Type
    if (columnType.TimeSeriesMeasureValueColumnInfo != null) {
        return parseTimeSeries(info, datum);
    }
    // If the column is of Array Type
    else if (columnType.ArrayColumnInfo != null) {
        const arrayValues = datum.ArrayValue;
        return `${info.Name}=${parseArray(info.Type.ArrayColumnInfo, arrayValues)}`;
    }
    // If the column is of Row Type
    else if (columnType.RowColumnInfo != null) {
        const rowColumnInfo = info.Type.RowColumnInfo;
        const rowValues = datum.RowValue;
        return parseRow(rowColumnInfo, rowValues);
    }
    // If the column is of Scalar Type
    else {
        return parseScalarType(info, datum);
    }
}

function parseTimeSeries(info, datum) {
    const timeSeriesOutput = [];
    datum.TimeSeriesValue.forEach(function (dataPoint) {
        timeSeriesOutput.push(`{time=${dataPoint.getTime()}, value=${parseDatum(info.Type.TimeSeriesMeasureValueColumnInfo, dataPoint.Value)}}`)
    });

    return `[${timeSeriesOutput.join(", ")}]`
}

function parseScalarType(info, datum) {
    return parseColumnName(info) + datum.ScalarValue;
}

function parseColumnName(info) {
    return info.Name == null ? "" : `${info.Name}=`;
}

function parseArray(arrayColumnInfo, arrayValues) {
    const arrayOutput = [];
    arrayValues.forEach(function (datum) {
        arrayOutput.push(parseDatum(arrayColumnInfo, datum));
    });
    return `[${arrayOutput.join(", ")}]`
}

module.exports = { getHistorical, getLatestWeather, getRainFall24, getLatestTempLogger, getAllRows };