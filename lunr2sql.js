// lunr2sql
// Construct FTS database from lunr index
var fs = require('fs');
var lunr = require('lunr');
var _ = require('underscore');

/**
 * Very basic single quote escaping
 */
function insecure_escape(value) {
    if (value !== null) return value.replace(/'/g, "''");
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
var skippedTokens = [];

corpusTokens.forEach(function (token) {
    if(token.length < 2) {
	skippedTokens.push(token);
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
console.log("(Skipped tokens were: " + JSON.stringify(skippedTokens) + ")");

if (placesTotal == foundTotal) {
    console.log("Successfully found tokens corresponding to all entries.");
} else {
    console.error("Failed to find tokens for all entries.");
    process.exit(2);
}

// Write out SQL
var output = "\
BEGIN TRANSACTION;\n\
CREATE TABLE places (ref varchar(255), json TEXT, PRIMARY KEY (ref));\n\
CREATE TABLE tokens (token varchar(255), ref varchar(255));\n\
";

var largestBlob = 0;

// Add place document insertion rows
_.each(places.all, function (element, index, list) {
    if (element.location === undefined) {
	console.warn(element.title + " is missing a location object");
    }

    var blob = insecure_escape(JSON.stringify(element));
    if(blob.length > largestBlob) largestBlob = blob.length;

    output += "INSERT INTO places VALUES ('"+insecure_escape(element.id)+"','"+blob+"');\n";
});

// Add token insertion rows
_.each(tokens2refs, function (element, index, list) {
    var token = insecure_escape(index);
    element.forEach(function (ref) {
	var ref = insecure_escape(ref);
	output += "INSERT INTO tokens VALUES ('"+token+"', '"+ref+"');\n";
    });
});

output += "COMMIT;\n";

fs.writeFileSync(outpath, output);
console.log("Wrote SQL file to " + outpath);
console.log("Biggest encountered blob size: " + largestBlob + ".");
