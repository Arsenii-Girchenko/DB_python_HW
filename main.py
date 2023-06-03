import psycopg2
import functions

db_params = {'db_name': 'db_python_hw', 'username': 'postgres', 'password': 'postgres'}

tables_dict = {'clients':['first_name', 'last_name', 'e_mail'],
               'phones': ['phone_number']}

if __name__ == '__main__':
    # functions.create_db(db_params)
    with psycopg2.connect(database=db_params['db_name'], user=db_params['username'], password=db_params['password']) as conn:
        with conn.cursor() as cur:
            # functions.create_main_tables(cur, tables_dict)
            # functions.create_connection_table(cur, 'clients', 'phones')
            # functions.add_client(cur, 1, "John", "Doe", "JohnDoe@mail.com")
            # functions.add_client(cur, 2, "Jane", "Doe", "JaneDoe@mail.com")
            # functions.add_phone_number(cur, 1, "111222333", 1,  1)
            # functions.update_client_data(cur, 1, "last_name", "Dough")
            # functions.remove_phone_number(cur, '111222333')
            # functions.remove_client(cur, 2)
            # functions.find_client(cur, "JohnDoe@mail.com")