// lunr2sql
// Construct FTS database from lunr index
var fs = require('fs');
var lunr = require('lunr');
var _ = require('underscore');

/**
 * Very basic single quote escaping
 */
function insecure_escape(value) {
    if (value !== null) return value.replace("'", "\\\'");
    else return value;
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
  campusName varchar(255),\n\
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
    id = insecure_escape(element.id);
    title = insecure_escape(element.title);
    campus_name = insecure_escape(element.campus_name);
    output += "INSERT INTO places(ref, title, campusName) VALUES ('"+id+"','"+title+"','"+campus_name+"');\n";
});

// Add token insertion rows
_.each(tokens2refs, function (element, index, list) {
    token = insecure_escape(index);
    element.forEach(function (ref) {
	ref = insecure_escape(ref);
	output += "INSERT INTO tokens(token, ref) VALUES ('"+token+"', '"+ref+"');\n";
    });
});

fs.writeFileSync(outpath, output);
console.log("Wrote SQL file to " + outpath);
