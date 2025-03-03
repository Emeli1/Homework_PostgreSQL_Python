import psycopg2
from pprint import pprint

# Удаление таблиц
def delete_db(cur):
    cur.execute("""
    DROP TABLE IF EXISTS clients, phone_numbers CASCADE;
    """)

# Функция, создающая структуру БД (таблицы).
def create_db(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS clients(
        id SERIAL PRIMARY KEY,
        first_name VARCHAR(30),
        last_name VARCHAR(30),
        email VARCHAR(254)
        );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS phone_numbers(
        phone VARCHAR(11) PRIMARY KEY,
        client_id INTEGER NOT NULL REFERENCES clients(id)
        );
    """)

# Функция, позволяющая добавить нового клиента.
def add_client(cur, first_name, last_name, email, phone=None):
    cur.execute("""
    INSERT INTO clients(first_name, last_name, email)
    VALUES(%s, %s, %s)
    """, (first_name, last_name, email))
    cur.execute("""
    SELECT id FROM clients
    ORDER BY id DESC
    LIMIT 1
    """)
    id = cur.fetchone()[0]
    if phone is None:
        return id
    else:
        add_phone(cur, phone, id)
        return id

# Функция, позволяющая добавить телефон для существующего клиента.
def add_phone(cur, phone, client_id):
    cur.execute("""
    INSERT INTO phone_numbers(phone, client_id)
    VALUES(%s, %s)
    """, (phone, client_id))
    return client_id

# Функция, позволяющая изменить данные о клиенте.
def update_client(cur, id, first_name=None, last_name=None, email=None):
    cur.execute("""
    SELECT * FROM clients
    WHERE id = %s
    """, (id,))

    info = cur.fetchone()
    if first_name is None:
        first_name = info[1]
    if last_name is None:
        last_name = info[2]
    if email is None:
        email = info[3]

    cur.execute("""
    UPDATE clients
    SET first_name = %s, last_name = %s, email = %s
    WHERE id = %s
    """, (first_name, last_name, email, id))
    return id

# Функция, позволяющая удалить телефон для существующего клиента.
def delete_phone(cur, phone):
    cur.execute("""
    DELETE FROM phone_numbers
    WHERE phone = %s
    """, (phone,))
    return phone

# Функция, позволяющая удалить существующего клиента.
def delete_client(cur, id):
    cur.execute("""
    DELETE FROM phone_numbers
    WHERE client_id = %s
    """, (id,))
    cur.execute("""
    DELETE FROM clients
    WHERE id = %s
    """, (id,))
    return id

# Функция, позволяющая найти клиента по его данным: имени, фамилии, email или телефону.
def find_client(cur, first_name=None, last_name=None, email=None, phone=None):
    if first_name is None:
        first_name = '%'
    else:
        first_name = '%' + first_name + '%'

    if last_name is None:
        last_name = '%'
    else:
        last_name = '%' + last_name + '%'

    if email is None:
        email = '%'
    else:
        email = '%' + email + '%'

    if phone is None:
        cur.execute("""
        SELECT c.id, c.first_name, c.last_name, c.email, p.phone FROM clients c
        LEFT JOIN phone_numbers p ON c.id = p.client_id
        WHERE c.first_name LIKE %s AND c.last_name LIKE %s
        AND c.email LIKE %s
        """, (first_name, last_name, email))
    else:
        cur.execute("""
        SELECT c.id, c.first_name, c.last_name, c.email, p.phone FROM clients c
        LEFT JOIN phone_numbers p ON c.id = p.client_id
        WHERE c.first_name LIKE %s AND c.last_name LIKE %s
        AND c.email LIKE %s AND p.phone LIKE %s
        """, (first_name, last_name, email, phone))
    return cur.fetchall()

if __name__ == '__main__':
    with psycopg2.connect(database="clients", user="postgres", password="postgres") as conn:
        with conn.cursor() as cur:
            # Удаление таблиц
            delete_db(cur)
            # Создание таблиц
            create_db(cur)
            print('БД создана')
            # Добавление клиентов
            print('Добавлен клиент id:',
                  add_client(cur, 'Иван', 'Иванов', 'ivanov11@gmail.com'))
            print('Добавлен клиент id:',
                  add_client(cur, 'Пётр', 'Петров', 'petrov22@gmail.com', 79112223344))
            print('Добавлен клиент id:',
                  add_client(cur, 'Вася', 'Пупкин', 'pypkin33@gmail.com', 79223334455))
            print('Добавлен клиент id:',
                  add_client(cur, 'Альберт', 'Альбертов', 'albertov44@gmail.com'))
            print('Добавлен клиент id:',
                  add_client(cur, 'Рудольф', 'Рудольфов', 'rudolfov55@gmail.com', 79334445566))

            print('Данные в таблицах:')
            cur.execute("""
            SELECT c.id, c.first_name, c.last_name, c.email, p.phone FROM clients c
            LEFT JOIN phone_numbers p ON c.id = p.client_id
            ORDER BY c.id
            """)
            pprint(cur.fetchall())

            # Добавление номера телефона
            print('Добавлен номер телефона клиенту id:',
                  add_phone(cur,79556667788, 1))
            print('Добавлен номер телефона клиенту id:',
                  add_phone(cur, 79778889911, 2))

            print('Данные в таблицах:')
            cur.execute("""
            SELECT c.id, c.first_name, c.last_name, c.email, p.phone FROM clients c
            LEFT JOIN phone_numbers p ON c.id = p.client_id
            ORDER BY c.id
            """)
            pprint(cur.fetchall())

            # Изменение данных клиента
            print('Изменениы данные клиента id:',
                  update_client(cur, 2, None, 'Петровский', 'petrovskii22@gmail.com'))

            # Удаление номера телефона клиента
            print('Удалён номер телефона:',
                  delete_phone(cur, '79556667788'))

            print('Данные в таблицах:')
            cur.execute("""
            SELECT c.id, c.first_name, c.last_name, c.email, p.phone FROM clients c
            LEFT JOIN phone_numbers p ON c.id = p.client_id
            ORDER BY c.id
            """)
            pprint(cur.fetchall())

            #Удаление клиента
            print('Удалён клиент с id:',
                  delete_client(cur, 3))

            print('Данные в таблицах:')
            cur.execute("""
            SELECT c.id, c.first_name, c.last_name, c.email, p.phone FROM clients c
            LEFT JOIN phone_numbers p ON c.id = p.client_id
            ORDER BY c.id
            """)
            pprint(cur.fetchall())

            #Поиск клиента
            print('Найден клиент по имени:')
            pprint(find_client(cur, 'Рудольф'))

            print('Найден клиент по фамилии:')
            pprint(find_client(cur, None, 'Альбертов', None))

            print('Найден клиент по email:')
            pprint(find_client(cur, None,None,'petrovskii22@gmail.com'))

            print('Найден клиент по номеру телефона:')
            pprint(find_client(cur, None, None, None, '79334445566'))

            print('Найден клиент по имени и email:')
            pprint(find_client(cur,'Иван', None, 'ivanov11@gmail.com'))








