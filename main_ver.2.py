import pymysql
from config import host, user, password,db_name
import pandas as pd
from sqlalchemy import create_engine
import colorama
from colorama import Fore, Back, Style
colorama.init()

def create_db(host, user, password, db_name):
    # creat DB
    try:
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            cursorclass=pymysql.cursors.DictCursor
        )
        print('Connection success \n', '_'*20, '\n')
        try:
            with connection.cursor() as cursor:
                cursor.execute(f'create database if not exists {db_name}')
                print(f'Database {db_name} created  \n',  '_'*20, '\n')
        finally:
            connection.close()
    except Exception as ex:
        print('Connection failed \n', ex, '\n', '_'*20, '\n')
        print(ex)
    return()


def table_create (df_table, name_table):
    # Create table
    df_table.to_sql(name = name_table, con = engine, if_exists = 'replace', index=False)
    print(f'Table {name_table} created ... \n')
    return ()

def SQL_query (query, host, user, password, db_name, num_query):
    # implementation SQL query
    try:
        connection = pymysql.connect(
            host = host,
            user = user,
            password = password,
            cursorclass=pymysql.cursors.DictCursor,
            database=db_name
        )
        print('_'*20, '\n')
        try:
            with connection.cursor() as cursor:
                cursor.execute(query)
                res_query = pd.DataFrame(cursor.fetchall())
                num_query = num_query+1
        finally:
            connection.close()
    except Exception as ex:
        print('Connection failed')
        print(ex)
    return res_query, num_query

def extract_name(url_file):
    # extract table name from file title
    ind_dot = url_file.find('.')
    ind_sl = url_file.rfind('/')
    name_table = url_file[ind_sl + 1:ind_dot]
    return name_table

def save_file (type_export, num_query, res_query):
    if type_export == 'json':
        res_query.to_json(f'data/query_{num_query}.json')
        file_place = (f'query {num_query}.json')
    else:
        res_query.to_xml(f'data/query_{num_query}.xml')
        file_place = (f'query {num_query}.xml')
    print(f'File {file_place} saved to data/ \n')
    return

num_query = 0

# create db
create_db(host, user, password, db_name)

engine = create_engine(f'mysql+mysqldb://root:Gznsqyf[05@localhost/{db_name}')

# input url, read file, extract name, create sql table

url_file = input('\033[5;28;34mInput URL rooms e.g. data/rooms.json \n ...  ')
name_table = extract_name(url_file)
df_table = pd.read_json(url_file)
table_create(df_table, name_table)

url_file = input(Fore.GREEN + Back.RED + 'Input URL rooms e.g. data/students.json \n ...  ')
name_table = extract_name(url_file)
df_table = pd.read_json(url_file)
table_create(df_table, name_table)
print('\033[0m')

# input file type

type_export = input('Please specify type of export json or xml...   ')

# num_query = 0
# res_query = ' '

# sql queries. implement, save result to file.

# список комнат и количество студентов в каждой из них
query = ('select room, count(id) as num from students')
res_query, num_query = SQL_query(query, host,user, password, db_name, num_query)
save_file(type_export, num_query, res_query)

# top 5 комнат, где самый маленький средний возраст студентов
query = ('select room, '
          'avg(datediff(utc_date, str_to_date(left(birthday, 10), "%Y-%c-%d"))) as avg_age '
          'from students '
          'group by room '
          'order by avg_age '
          'limit 5')
res_query, num_query = SQL_query(query, host,user, password, db_name, num_query)
save_file(type_export, num_query, res_query)

# top 5 комнат с самой большой разницей в возрасте студентов
query = ('select room, '
        '(max(datediff(utc_date, str_to_date(left(birthday, 10), "%Y-%m-%d"))) '
        '- min(datediff(utc_date, str_to_date(left(birthday, 10), "%Y-%m-%d")))) as age_diff '
        'from students '
        'group by room '
        'order by age_diff desc '
        'limit 5'
        )
res_query, num_query = SQL_query(query, host,user, password, db_name, num_query)
save_file(type_export, num_query, res_query)

# список комнат где живут разнополые студенты
query = ('select room, '
        'count(distinct (sex)) as M_F '
        'from students '
        'group by room '
        'having M_F = 2 '
        'order by room')
res_query, num_query = SQL_query(query, host,user, password, db_name, num_query)
save_file(type_export, num_query, res_query)



