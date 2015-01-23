lunr2sql
========
Create a SQLite database for client-side search of the [places index](https://github.com/oss/placesindex).
Retains the custom [lunr](https://github.com/olivernn/lunr.js) tokens (special abbreviations, etc.).

#### Usage
```
npm install
wget https://../places.txt
node lunr2sql
sqlite3 places.db < out.sql
```

