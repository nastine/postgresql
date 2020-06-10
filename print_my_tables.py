
import psycopg2 as pg
import pandas as pd


conn = pg.connect(database='netology', user='netology', password='netology',
                host='localhost', port=5432)
print(pd.read_sql('select * from students', conn))
print(pd.read_sql('select * from courses', conn))
print(pd.read_sql('select * from student_course', conn))
