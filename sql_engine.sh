#!/bin/bash
# python3 query_parser.py "$1"
python3 sql_engine.py 'select * from table1'
python3 sql_engine.py 'select max(A) from table1'
python3 sql_engine.py 'select count(A) from table1'
python3 sql_engine.py 'select avg(A) from table1'
python3 sql_engine.py 'select A, B from table1'
python3 sql_engine.py 'select distinct A, B from table1'
python3 sql_engine.py 'select * from table2, table1 where table1.A = table2.B'
python3 sql_engine.py 'select A, B from table1, table2 where table1.A > 10 AND table2.B > 2'
