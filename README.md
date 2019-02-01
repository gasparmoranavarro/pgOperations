This library allow you to perform the most common operations with PostGIS:
    - Connection, insert, delete, update, select, create and drop databases

You first have to get a object of the classs pgConnect. Later you can create
and object pgOperations to perform the operations. The pgOperations methods are
applicable to any database and any table, with or without any type of geometry

This library depends of the psycopg2 library (http://initd.org/psycopg/)
This library uses Python 2.7

