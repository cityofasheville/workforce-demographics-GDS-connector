var cc = DataStudioApp.createCommunityConnector();
var userProperties = PropertiesService.getUserProperties();

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
    /**
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
     **/
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
        .setType(types.TEXT);

    fields
        .newDimension()
        .setId('position_races')
        .setName('Race')
        .setType(types.TEXT);

    return fields;
}

// https://developers.google.com/datastudio/connector/reference#getschema
function getSchema(request) {
    try {
        return {schema: getFields().build()};
    } catch(e) {
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
    request.fields.forEach(function(field) {
        for (var i = 0; i < schema['schema'].length; i++) {
            if (schema['schema'][i].name === field.name) {
                dataSchema.push(schema['schema'][i]);
                break;
            }
        }
    });
    try {
        // get the fields requested from the request object
        /**
         var colNames = request.fields.map(function (field) {
            return field.name
        });
         **/
            // compose query string
        var recordquery = '{"query": "{ internal_demographics_by_position {Department age data_id est_years_of_service ethnicity_hispanic gender position position_races {race}} }"}';
        //console.log(recordquery)
        // create the fetch object
        var fetchoptions = {
            'method': 'post',
            'payload': recordquery,
            'contentType': 'application/json'
        };

        //store url in properties for now
        var properties = PropertiesService.getScriptProperties();
        var url = properties.getProperty('apiurl');
        var response = UrlFetchApp.fetch(url, fetchoptions);
        // console.log(response)
        var parsedResponse = JSON.parse(response).data.internal_demographics_by_position;

        // console.log(JSON.stringify(parsedResponse))

        // filter for requested fields
        // var requestedData = parsedResponse.map(function(item) {
        var requestedData = [];
        parsedResponse.forEach(function(item) {
            //console.log(item.gender)
            // set up our return array
            var values = [];
            var race = '';
            var races = [];
            // loop through the requested fields
            dataSchema.forEach(function(field) {
                //console.log(field.name);
                switch (field.name) {
                    case 'position_races':
                        item.position_races.forEach(function(race) {
                            races.push(race.race)
                            //console.log(races.join());
                        });
                        values.push(races.join());
                        break;
                    case 'Department':
                        values.push(item.Department);
                        break;
                    case 'position':
                        values.push(item.position);
                        break;
                    case 'gender':
                        values.push(item.gender);
                        break;
                    case 'ethnicity_hispanic':
                        values.push(item.ethnicity_hispanic);
                        break;
                    case 'age':
                        values.push(item.age);
                        break;
                    case 'est_years_of_service':
                        values.push(item.est_years_of_service);
                        break;
                    case 'data_id':
                        values.push(item.data_id);
                        break;
                    default:
                        values.push('');
                }
            });

            requestedData.push({
                values: values
            });
            //console.log(values);
            return requestedData;
        });


        console.log(JSON.stringify(dataSchema,requestedData))
        return {
            schema: dataSchema,
            rows: requestedData
        };

    } catch (e) {
        console.log(e);
    }

}
