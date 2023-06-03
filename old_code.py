import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def create_db(db_params):
    try:
        connection = psycopg2.connect(user=db_params['username'], password=db_params['password'])
        connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        with connection.cursor() as cursor:
            cursor.execute('CREATE DATABASE %s' % (db_params['db_name'], ))
    finally:
        if connection:
            connection.close()
            return print('Database created')    


def create_main_tables(db_params, tables_dict): 
    with psycopg2.connect(database=db_params['db_name'], user=db_params['username'], password=db_params['password']) as conn:
        with conn.cursor() as cur:
            for t_name, t_columns in tables_dict.items():
                create_table = '''
                    CREATE TABLE IF NOT EXISTS %s(
                        %s_id SERIAL PRIMARY KEY                    
                    );'''
                cur.execute(create_table % (t_name, t_name))
                for column_name in t_columns:
                    add_columns = '''
                    ALTER TABLE %s
                    ADD %s VARCHAR(100) NOT NULL;
                    '''
                    cur.execute(add_columns % (t_name, column_name))
    return print('Tables created')

def create_connection_table(db_params, first_table_name, second_table_name):
    with psycopg2.connect(database=db_params['db_name'], user=db_params['username'], password=db_params['password']) as conn:
        with conn.cursor() as cur:
            create_connection_table = '''
                CREATE TABLE IF NOT EXISTS %s%s(
                    connection_id SERIAL PRIMARY KEY,
                    %s_id INTEGER REFERENCES %s(%s_id),
                    %s_id INTEGER REFERENCES %s(%s_id)
                );
                '''
            cur.execute(create_connection_table % 
                        (first_table_name, second_table_name,
                         first_table_name, first_table_name, first_table_name,
                         second_table_name, second_table_name, second_table_name))
    return print('Connection table created')

def add_client(db_params, new_client_first_name, new_client_last_name, new_client_e_mail):
    with psycopg2.connect(database=db_params['db_name'], user=db_params['username'], password=db_params['password']) as conn:
        with conn.cursor() as cur:
            add_client = '''
                INSERT INTO clients(first_name, last_name, e_mail) VALUES(    
                    %s, %s, %s
                );
                '''
            cur.execute(add_client % (new_client_first_name, new_client_last_name, new_client_e_mail))
    return print(f'Client {new_client_first_name} {new_client_last_name} with email: {new_client_e_mail} added to the database')
        
def add_phone_number(db_params, new_phone_number, client_id):
    with psycopg2.connect(database=db_params['db_name'], user=db_params['username'], password=db_params['password']) as conn:
        with conn.cursor() as cur:
            add_phone_number = '''
                INSERT INTO phones(phone_number) VALUES(
                    %s
                ) RETURNING phones_id;
                '''
            cur.execute(add_phone_number % (new_phone_number,))
            new_id = cur.fetchone()[0]
            add_to_connection = '''
                INSERT INTO clientsphones(clients_id, phones_id) VALUES(
                    %s, %s
                )
                '''
            cur.execute(add_to_connection % (client_id, new_id))
    return print(f'Phone number added')
        
def update_client_data(db_params, client_id, row_name, new_value):
    with psycopg2.connect(database=db_params['db_name'], user=db_params['username'], password=db_params['password']) as conn:
        with conn.cursor() as cur:
            update_client_data = '''
                UPDATE clients SET %s=%s WHERE clients_id=%s;
                '''
            cur.execute(update_client_data % (row_name, new_value, client_id))
    return print(f'Client info changed')

def remove_phone_number(db_params, phone_number):
    with psycopg2.connect(database=db_params['db_name'], user=db_params['username'], password=db_params['password']) as conn:
        with conn.cursor() as cur:
            del_connection = '''
                DELETE FROM clientsphones
                    WHERE phones_id = (SELECT cp.phones_id
                    FROM clientsphones cp
                    JOIN phones p ON cp.phones_id = p.phones_id
                    WHERE p.phone_number = %s);
                '''
            cur.execute(del_connection, (phone_number,))
            del_phone = '''
                DELETE FROM phones
                    WHERE phone_number = %s;
                '''
            cur.execute(del_phone, (phone_number,))
    return print(f'Phone number deleted')

def remove_client(db_params, client_id):
    with psycopg2.connect(database=db_params['db_name'], user=db_params['username'], password=db_params['password']) as conn:
        with conn.cursor() as cur:
            del_connection = '''
                DELETE FROM clientsphones
                    WHERE clients_id = (SELECT cp.clients_id
                    FROM clientsphones cp
                    JOIN clients c ON cp.clients_id = c.clients_id
                    WHERE c.clients_id = %s);
                '''
            cur.execute(del_connection % (client_id,))
            del_client = '''
                DELETE FROM clients
                    WHERE clients_id = %s;
                '''
            cur.execute(del_client % (client_id,))
        return print(f'Client removed')
    
def find_client(db_params, first_name, last_name, e_mail, phone_number):
    with psycopg2.connect(database=db_params['db_name'], user=db_params['username'], password=db_params['password']) as conn:
        with conn.cursor() as cur:
            find_client = '''
            SELECT c.clients_id FROM clients c
                WHERE (c.first_name = %s 
                AND c.last_name = %s
                AND c.e_mail = %s)
                OR (c.clients_id = (SELECT c.clients_id
                    FROM clients c
                    JOIN clientsphones cp ON c.clients_id = cp.clients_id
                    JOIN phones p ON cp.phones_id = p.phones_id
                    WHERE p.phone_number = %s));
            '''
            cur.execute(find_client % (first_name, last_name, e_mail, phone_number))
            result = cur.fetchone()
    return print(f'CLient\'s ID: {result[0]}')


db_params = {'db_name': 'db_python_hw', 'username': 'postgres', 'password': 'postgres'}

tables_dict = {'clients':['first_name', 'last_name', 'e_mail'],
               'phones': ['phone_number']}

# create_db(db_params)
# create_main_tables(db_params, tables_dict)
# create_connection_table(db_params, 'clients', 'phones')
# add_client(db_params, "'John'", "'Doe'", "'JohnDoe@mail.com'")
# add_client(db_params, "'Jane'", "'Doe'", "'JaneDoe@mail.com'")
# add_phone_number(db_params, "'111222333'", 1)
# update_client_data(db_params, 1, 'last_name', "'Dough'")
# remove_phone_number(db_params, '111222333')
# remove_client(db_params, 2)
find_client(db_params, "'John'", "'Dough'", "'JohnDoe@mail.com'", "'111222333'")