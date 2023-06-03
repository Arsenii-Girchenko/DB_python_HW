import psycopg2
from psycopg2 import sql
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

def create_main_tables(cursor, tables_dict): 
    for t_name, t_columns in tables_dict.items():
        create_table = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {t_name}(
                id SERIAL PRIMARY KEY
            )""").format(
                t_name = sql.Identifier(t_name),
            )
        cursor.execute(create_table)
        for column_name in t_columns:
            add_columns = sql.SQL("""
                ALTER TABLE {t_name}
                ADD {column_name} VARCHAR(100) NOT NULL
                """).format(
                    t_name = sql.Identifier(t_name),
                    column_name = sql.Identifier(column_name),
                )
            cursor.execute(add_columns)
    return print('Tables created')

def create_connection_table(cursor, first_table_name, second_table_name):
    create_connection_table = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {connection_table_name}(
            id INTEGER PRIMARY KEY,
            {first_table_id} INTEGER REFERENCES {first_table_name}(id),
            {second_table_id} INTEGER REFERENCES {second_table_name}(id)
        )
        """).format(
            connection_table_name = sql.Identifier(first_table_name + second_table_name),
            first_table_name = sql.Identifier(first_table_name),
            second_table_name = sql.Identifier(second_table_name),
            first_table_id = sql.Identifier(first_table_name + '_id'),
            second_table_id = sql.Identifier(second_table_name + '_id',)
        )
    cursor.execute(create_connection_table)
    return print('Connection table created')

def add_client(cursor, new_client_id: int, new_client_first_name: str, new_client_last_name: str, new_client_e_mail: str):
    add_client = sql.SQL("""
        INSERT INTO {clients} VALUES(    
            {client_id}, {client_first_name}, {client_last_name}, {client_e_mail}
        )
        """).format(
            clients = sql.Identifier('clients'),
            client_id = sql.Literal(new_client_id),
            client_first_name = sql.Literal(new_client_first_name),
            client_last_name = sql.Literal(new_client_last_name),
            client_e_mail = sql.Literal(new_client_e_mail),
            )
    cursor.execute(add_client)
    return print(f'Client {new_client_first_name} {new_client_last_name} with email: {new_client_e_mail} added to the database')
     
def add_phone_number(cursor, new_phone_id, new_phone_number, connection_id, client_id):
    add_phone_number = sql.SQL("""
        INSERT INTO {phones} VALUES(
            {phone_id}, {phone_number}
        )
        """).format(
            phones = sql.Identifier('phones'),
            phone_id = sql.Literal(new_phone_id),
            phone_number = sql.Literal(new_phone_number),
        )
    cursor.execute(add_phone_number)
    add_to_connection = sql.SQL("""
        INSERT INTO {connection_table_name} VALUES(
           {connection_id}, {clients_id}, {phones_id}
        )
        """).format(
            connection_table_name = sql.Identifier('clientsphones'),
            connection_id = sql.Literal(connection_id),
            clients_id = sql.Literal(client_id),
            phones_id = sql.Literal(new_phone_id),
        )
    cursor.execute(add_to_connection)
    return print(f'Phone number added')
        
def update_client_data(cursor, client_id, row_name, new_value):
    update_client_data = sql.SQL("""
        UPDATE {table_name} SET {row_name}={new_value} WHERE id={client_id};
        """).format(
            table_name = sql.Identifier('clients'),
            row_name = sql.Identifier(row_name),
            new_value = sql.Literal(new_value),
            client_id = sql.Literal(client_id),
        )
    cursor.execute(update_client_data)
    return print(f'Client info changed')

def remove_phone_number(cursor, phone_number):
    del_connection = sql.SQL("""
        DELETE FROM clientsphones
            WHERE phones_id = (SELECT cp.phones_id
            FROM clientsphones cp
            JOIN phones p ON cp.phones_id = p.id
            WHERE p.phone_number = {phone_number});
        """).format(
            phone_number = sql.Literal(phone_number),
        )
    cursor.execute(del_connection)
    del_phone = sql.SQL("""
        DELETE FROM phones
            WHERE phone_number = {phone_number};
        """).format(
            phone_number = sql.Literal(phone_number),
        )
    cursor.execute(del_phone)
    return print(f'Phone number deleted')

def remove_client(cursor, client_id):
    del_connection = sql.SQL("""
        DELETE FROM clientsphones
            WHERE clients_id = (SELECT cp.clients_id
            FROM clientsphones cp
            JOIN clients c ON cp.clients_id = c.id
            WHERE c.id = {client_id});
        """).format(
            client_id = sql.Literal(client_id),
        )
    cursor.execute(del_connection)
    del_client = sql.SQL("""
        DELETE FROM clients
            WHERE id = {client_id};
        """).format(
            client_id = sql.Literal(client_id),
        )
    cursor.execute(del_client)
    return print(f'Client removed')
    
# def find_client(cursor, first_name, last_name, e_mail, phone_number):
#     find_client = sql.SQL("""
#         SELECT c.clients_id FROM clients c
#             WHERE (c.first_name = {first_name} 
#             AND c.last_name = {last_name}
#             AND c.e_mail = {e_mail})
#             OR (c.id = (SELECT c.id
#                 FROM clients c
#                 JOIN clientsphones cp ON c.id = cp.clients_id
#                 JOIN phones p ON cp.phones_id = p.id
#                 WHERE p.phone_number = {phone_number}));
#         """).format(
            
#         )
#     cursor.execute(find_client)
#     result = cursor.fetchone()
#     return print(f'CLient\'s ID: {result[0]}')

def find_client(cursor, client_parameter):
    find_client = sql.SQL("""
        SELECT c.id FROM clients c
            WHERE (c.first_name = {client_parameter} 
            OR c.last_name = {client_parameter}
            OR c.e_mail = {client_parameter})
            OR (c.id = (SELECT c.id
                FROM clients c
                JOIN clientsphones cp ON c.id = cp.clients_id
                JOIN phones p ON cp.phones_id = p.id
                WHERE p.phone_number = {client_parameter}));
        """).format(
            client_parameter = sql.Literal(client_parameter),
        )
    cursor.execute(find_client)
    result = cursor.fetchone()
    return print(f'CLient\'s ID: {result[0]}')