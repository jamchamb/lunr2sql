// lunr2sql
// Construct FTS database from lunr index
var fs = require('fs');
var lunr = require('lunr');
var _ = require('underscore');

/**
 * Very basic single quote escaping
 */
function insecure_escape(value) {
    if (value !== null) return value.replace(/'/g, "\\\'");
    else return value;
}

/**
 * Recursively apply insecure_escape to strings in an object
 */
function insecurely_escaped_copy(object) {
    var copy = {};
    _.each(object, function (element, index, list) {
	if (typeof element === 'string') {
	    copy[index] = insecure_escape(element);
	} else if (typeof element === 'object') {
	    copy[index] = insecurely_escaped_copy(element);
	} else {
	    copy[index] = element;
	}
    });
    return copy;
}

var inpath = "places.txt";
var outpath = "out.sql";

try {
    var places = JSON.parse(fs.readFileSync(inpath, 'utf8'));
} catch (err) {
    console.error(err);
    process.exit(1);
}

index = lunr.Index.load(places.lunr);

// All tokens
var corpusTokens = places.lunr.corpusTokens;

// Title tokens, including custom abbreviations
var documentStore = places.lunr.documentStore;

var placesTotal = documentStore.length;
var found = {}; // table of found refs
var tokens2refs = {}; // table of token->[ref,ref,ref,...]

corpusTokens.forEach(function (token) {
    if(token.length < 2) {
	console.log("Skipping over token \"" + token + "\"");
	return;
    }

    var results = index.search(token);

    results.forEach(function (result) {
	if (tokens2refs[token] === undefined) {
	    tokens2refs[token] = [result.ref];
	} else {
	    tokens2refs[token].push(result.ref);
	}
	found[result.ref] = "1";
    });
});

var foundTotal = _.size(found);

// Verify that all documents will have a corresponding token so that they can be found.
console.log("Document store size: " + placesTotal);
console.log("Token iteration recovered " + foundTotal + " unique entries.");
if (placesTotal == foundTotal) {
    console.log("Successfully found tokens corresponding to all entries.");
} else {
    console.err("Failed to find tokens for all entries.");
    process.exit(2);
}

// Write out SQL
var output = "\
DROP TABLE IF EXISTS places, tokens;\n\
CREATE TABLE places (\n\
  ref varchar(255),\n\
  title varchar(255),\n\
  building_id varchar(10),\n\
  building_number varchar(10),\n\
  cid varchar(10),\n\
  description varchar(2048),\n\
  campus_code varchar(2),\n\
  campus_name varchar(255),\n\
  offices varchar(2048),\n\
  location_name varchar(255),\n\
  location_street varchar(255),\n\
  location_state varchar(255),\n\
  location_state_abbr varchar(2),\n\
  location_country varchar(255),\n\
  location_country_abbr varchar(5),\n\
  location_country_additional varchar(255),\n\
  location_postal_code varchar(10),\n\
  location_latitude FLOAT,\n\
  location_longitude FLOAT,\n\
  PRIMARY KEY (ref)\n\
);\n\
\n\
CREATE TABLE tokens (\n\
  token varchar(255),\n\
  ref varchar(255)\n\
);\n\
\n";

// Add place document insertion rows
_.each(places.all, function (element, index, list) {
    // Escape single quotes in all fields.
    var copy = insecurely_escaped_copy(element);

    if (copy.location === undefined) {
	console.log(copy.title + " is missing a location object");
	copy.location = {};
    }

    var offices = "";
    if (element.offices !== undefined) {
	offices = insecure_escape(JSON.stringify(element.offices));
    }

    output += "INSERT INTO places VALUES ('"+copy.id+"','"+copy.title+"','"+copy.building_id+"','"+copy.building_number+"'," +
	"'"+copy.cid+"','"+copy.description+"','"+copy.campus_code+"','"+copy.campus_name+"','"+offices+"'," +
	"'"+copy.location.name+"','"+copy.location.street+"','"+copy.location.state+"','"+copy.location.state_abbr+"'," + 
	"'"+copy.location.country+"','"+copy.location.country_abbr+"','"+copy.location.additional+"','"+copy.location.postal_code+"'," + 
	"'"+copy.location.latitude+"','"+copy.location.longitude+"');\n";
});

// Add token insertion rows
_.each(tokens2refs, function (element, index, list) {
    var token = insecure_escape(index);
    element.forEach(function (ref) {
	var ref = insecure_escape(ref);
	output += "INSERT INTO tokens(token, ref) VALUES ('"+token+"', '"+ref+"');\n";
    });
});

fs.writeFileSync(outpath, output);
console.log("Wrote SQL file to " + outpath);
