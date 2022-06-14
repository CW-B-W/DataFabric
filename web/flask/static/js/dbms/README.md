# How to add new DBMS module

## For example Adding MySQL module
1. Create a .js file named `mysql.js`  
***Note that the name must be lowercase***

2. Implement functions in `mysql.js`, e.g.  
***The function names should have suffix `_mysql`***
```javascript
function generateSrcInfo_mysql(ip, port, username, password, dbName, tableName, columns, namemapping, startTime, endTime, timeColumn) {
    ...
}

...
```

3. That's all