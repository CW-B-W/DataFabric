# How to add new DBMS module

## For example Adding MongoDB module
1. Create a .js file named `mongodb.js`  
***Note that the name must be lowercase***

2. Implement functions in `mongodb.js`, e.g.  
***The function names should have suffix `_mongodb`***
```javascript
function generateSrcInfo_mongodb(ip, port, username, password, dbName, tableName, columns, namemapping, startTime, endTime, timeColumn) {
    ...
}

...
```

3. That's all