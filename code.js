var cc = DataStudioApp.createCommunityConnector();

// https://developers.google.com/datastudio/connector/reference#getauthtype
function getAuthType() {
    var AuthTypes = cc.AuthType;
    return cc
        .newAuthTypeResponse()
        .setAuthType(AuthTypes.NONE)
        .build();
}

function isAdminUser() {
    return true;
};

// https://developers.google.com/datastudio/connector/reference#getconfig
function getConfig() {
    var config = cc.getConfig();

    config
        .newInfo()
        .setId('instructions')
        .setText('NOTE: get better language here --- This is the City of Asheville Workforce demographic data. Only City of Asheville users are authorized to connect to this data. Any reports generated should be reviewed by the Data or Equity team before release.');

    return config.build();
}

function getFields() {
    var fields = cc.getFields();
    var types = cc.FieldType;
    var aggregations = cc.AggregationType;

    fields
        .newDimension()
        .setId('Department')
        .setName('Department')
        .setType(types.TEXT);

    fields
        .newDimension()
        .setId('position')
        .setName('Position')
        .setType(types.TEXT);

    fields
        .newDimension()
        .setId('gender')
        .setName('Gender')
        .setType(types.TEXT);

    fields
        .newMetric()
        .setId('ethnicity_hispanic')
        .setName('Hispanic')
        .setGroup('Ethnicity')
        .setType(types.BOOLEAN);

    fields
        .newMetric()
        .setId('race_asian')
        .setName('Asian')
        .setGroup('Race')
        .setType(types.BOOLEAN);

    fields
        .newMetric()
        .setId('race_black')
        .setName('Black')
        .setGroup('Race')
        .setType(types.BOOLEAN);

    fields
        .newMetric()
        .setId('race_native')
        .setName('Native')
        .setGroup('Race')
        .setType(types.BOOLEAN);

    fields
        .newMetric()
        .setId('race_pacific')
        .setName('Pacific')
        .setGroup('Race')
        .setType(types.BOOLEAN);

    fields
        .newMetric()
        .setId('race_white')
        .setName('White')
        .setGroup('Race')
        .setType(types.BOOLEAN);

    fields
        .newMetric()
        .setId('age')
        .setName('Age')
        .setType(types.NUMBER);

    fields
        .newMetric()
        .setId('est_years_of_service')
        .setName('Estimated Years of Service')
        .setType(types.NUMBER);

    fields
        .newDimension()
        .setId('data_id')
        .setName('Data Identifier')
        .setType(types.TEXT)

    return fields;
}

// https://developers.google.com/datastudio/connector/reference#getschema
function getSchema(request) {
    try {
        return {schema: getFields().build()};
    } catch (e) {
        console.log(e)
    }
}

// https://developers.google.com/datastudio/connector/reference#getdata
function getData(request) {
    //set up schema array
    var dataSchema = [];
    //get our schema
    var schema = getSchema(request)
    // Sort and filter by requested fields
    request.fields.forEach(function (field) {
        for (var i = 0; i < schema['schema'].length; i++) {
            if (schema['schema'][i].name === field.name) {
                dataSchema.push(schema['schema'][i]);
                break;
            }
        }
    });

    try {
        // get the fields requested from the request object
        var colNames = request.fields.map(function (field) {
            return field.name
        });
        // compose query string
        var recordquery = '{"query": "{ internal_demographics_by_position {' + colNames.join(' ') + '}}"}';
        // create the fetch object
        var fetchoptions = {
            'method': 'post',
            'payload': recordquery,
            'contentType': 'application/json'
        };

        //store url in properties for now
        var url = PropertiesService.getScriptProperties().getProperty('url');
        var response = UrlFetchApp.fetch(url, fetchoptions);
        //console.log(response)
        var parsedResponse = JSON.parse(response).data.internal_demographics_by_position;

        // filter for requested fields
        var requestedData = parsedResponse.map(function (item) {
            // set up our return array
            var values = []
            // loop through the requested fields
            dataSchema.forEach(function (field) {
                if (!!item[field.name]) {
                    values.push(item[field.name]);
                } else {
                    values.push('');
                }
            });

            return {values: values};
        });
        console.log(JSON.stringify(requestedData))
        return {
            schema: dataSchema,
            rows: requestedData
        };

    } catch (e) {
        console.log(e);
    }

}
