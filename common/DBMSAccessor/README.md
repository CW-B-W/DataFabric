# How to add new DBMSAccessor module

## For example Adding MySQL module
1. Make a directory named 'mysqlAccessor'.  
***Note that the 'mysql' must be lowercase***
```bash
mkdir mysqlAccessor
```

2. Add `__init__.py` and `mysqlAccessor.py` in the directory
```bash
cd mysqlAccessor
touch __init__.py
touch mysqlAccessor.py
```

3. Edit `mysqlAccessor.py` and implement functions  
***The function names should have suffix `_mysql`***
```python
import ...

def preview_table_mysql(username, password, ip, port, db, table, limit):
    ...

def query_table_mysql(username, password, ip, port, db, table, columns, start_time, end_time, time_column):
    ...

...
```

4. That's all