

class Creating_db:
    def create_db(host, user, password, db_name):
        # creat DB
        try:
            connection = pymysql.connect(
                host=host,
                user=user,
                password=password,
                cursorclass=pymysql.cursors.DictCursor
            )
            print('Connection success \n', '_' * 20, '\n')
            try:
                with connection.cursor() as cursor:
                    cursor.execute(f'create database if not exists {db_name}')
                    print(f'Database {db_name} created  \n', '_' * 20, '\n')
            finally:
                connection.close()
        except Exception as ex:
            print('Connection failed \n', ex, '\n', '_' * 20, '\n')
            print(ex)
        return ()