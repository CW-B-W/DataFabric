# How to add new DBMSAccessor module

## For example Adding MongoDB module
1. Make a directory named 'mongodbAccessor'.  
***Note that the 'mongodb' must be lowercase***
```bash
mkdir mongodbAccessor
```

2. Add `__init__.py` and `mongodbAccessor.py` in the directory
```bash
cd mongodbAccessor
touch __init__.py
touch mongodbAccessor.py
```

3. Edit `mongodbAccessor.py` and implement functions  
***The function names should have suffix `_mongodb`***
```python
import ...

def preview_table_mongodb(username, password, ip, port, db, table, limit):
    ...

def query_table_mongodb(username, password, ip, port, db, table, columns, start_time, end_time, time_column):
    ...

...
```

4. That's all