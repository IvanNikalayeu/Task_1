
import pymysql
from config import host, user, password,db_name
import pandas as pd
from sqlalchemy import create_engine

# create DB
try:
    connection = pymysql.connect(
        host=host,
        user = user,
        password=password,
        cursorclass=pymysql.cursors.DictCursor
    )
    print('Connection success')
    print()

    try:
        with connection.cursor() as cursor:
            cursor.execute('create database if not exists task_1')
            print('Database task_1 created')
    finally:
        connection.close()
except Exception as ex:
    print('Connection refused')
    print(ex)
#
url_rooms = input("Input URL rooms e.g. data/rooms.json ...  ")
url_students = input("Input URL students e.g. data/students.json ... ")
type_export = input('Please specify type of export json or xml...   ')

# считываение файлов

df_rooms = pd.read_json(url_rooms)
df_students = pd.read_json(url_students)

# read jsons to df

# df_rooms = pd.read_json('data/rooms.json')
# df_students = pd.read_json('data/students.json')

# put df to sql tables

engine = create_engine('mysql+mysqldb://root:Gznsqyf[05@localhost/task_1')
df_rooms.to_sql('rooms', con = engine, if_exists='replace', index=False)
df_students.to_sql('students', con = engine, if_exists='replace', index=False)
print('Tables created')

# SQL queries

try:
    connection = pymysql.connect(
        host=host,
        user = user,
        password=password,
        cursorclass=pymysql.cursors.DictCursor,
        database=db_name
    )
    print('Connection success')
    print()

    try:
        with connection.cursor() as cursor:
            cursor.execute("select room, count(id) as num from students")
            rows_1 = cursor.fetchall()

            cursor.execute('select room, avg(datediff(utc_date, str_to_date(left(birthday, 10), "%Y-%c-%d"))) as avg_age from students '
                           'group by room '
                           'order by avg_age '
                           'limit 5'
                           )
            rows_2 = cursor.fetchall()

            cursor.execute('select room, '
                           '(max(datediff(utc_date, str_to_date(left(birthday, 10), "%Y-%m-%d"))) '
                           '- min(datediff(utc_date, str_to_date(left(birthday, 10), "%Y-%m-%d")))) as age_diff '
                           'from students '
                           'group by room '
                           'order by age_diff desc '
                           'limit 5'
                           )
            rows_3 = cursor.fetchall()

            cursor.execute('select room, '
                           'count(distinct (sex)) as M_F '
                           'from students '
                           'group by room '
                           'having M_F = 2 '
                           'order by room')
            rows_4 = cursor.fetchall()

    finally:
        connection.close()
except Exception as ex:
    print('Connection refused')
    print(ex)

df_1 = pd.DataFrame(rows_1)
df_2 = pd.DataFrame(rows_2)
df_3 = pd.DataFrame(rows_3)
df_4 = pd.DataFrame(rows_4)

if type_export == 'json':
    place_1 = 'data/query_1.json'
    place_2 = 'data/query_2.json'
    place_3 = 'data/query_3.json'
    place_4 = 'data/query_4.json'
    df_1.to_json(place_1)
    df_2.to_json(place_2)
    df_3.to_json(place_3)
    df_4.to_json(place_4)
    print(f'Queries exported to: \n {place_1} \n {place_2} \n {place_3} \n {place_4}')
elif type_export == 'xml':
    place_1 = 'data/query_1.xml'
    place_2 = 'data/query_2.xml'
    place_3 = 'data/query_3.xml'
    place_4 = 'data/query_4.xml'
    df_1.to_xml(place_1)
    df_2.to_xml(place_2)
    df_3.to_xml(place_3)
    df_4.to_xml(place_4)
    print(f'Queries exported to: \n {place_1} \n {place_2} \n {place_3} \n {place_4}')