const constants = require('./constants');

const HOSTNAME = "host1";

// See records ingested into this table so far
const SELECT_ALL_QUERY = "SELECT * FROM \"" + constants.DATABASE_NAME + "\".\"" + constants.TEMP_LOGGER_TABLE_NAME + "\"";

// Note names of fields - important for conversion routine
const SELECT_LATEST_WEATHER = "SELECT measure_name, to_milliseconds(time) AS unixtime, measure_value::varchar as value FROM \"" + constants.DATABASE_NAME + "\".\"" + constants.WEATHER_DATA_TABLE_NAME + "\" WHERE time between ago(15m) and now() ORDER BY time DESC LIMIT 20"

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

module.exports = { getLatestWeather, getAllRows };