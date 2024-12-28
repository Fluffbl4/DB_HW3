import psycopg2

def create_db(conn):
    with conn.cursor() as cursor:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS clients (
                id SERIAL PRIMARY KEY,
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                email VARCHAR(255) UNIQUE
            );
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS phones (
                id SERIAL PRIMARY KEY,
                client_id INTEGER,
                number VARCHAR(20),
                CONSTRAINT fk_client
                    FOREIGN KEY(client_id)
                    REFERENCES clients(id)
                    ON DELETE CASCADE
            );
        ''')
        conn.commit()


def add_client(conn, first_name, last_name, email, phones=None):
    with conn.cursor() as cursor:
        cursor.execute('''INSERT INTO clients (first_name, last_name, email) 
                          VALUES (%s, %s, %s) RETURNING id''',
                       (first_name, last_name, email))
        client_id = cursor.fetchone()[0]
        conn.commit()

        if phones:
            for phone in phones:
                add_phone(conn, client_id, phone)

        return client_id


def add_phone(conn, client_id, phone):
    with conn.cursor() as cursor:
        cursor.execute('''INSERT INTO phones (client_id, number) 
                          VALUES (%s, %s)''', (client_id, phone))
        conn.commit()


def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    with conn.cursor() as cursor:
        updates = []
        values = []

        if first_name:
            updates.append("first_name=%s")
            values.append(first_name)

        if last_name:
            updates.append("last_name=%s")
            values.append(last_name)

        if email:
            updates.append("email=%s")
            values.append(email)

        if updates:
            query = "UPDATE clients SET " + ", ".join(updates) + " WHERE id=%s"
            values.append(client_id)
            cursor.execute(query, tuple(values))
            conn.commit()

        if phones:
            # Сначала удаляем старые телефоны
            delete_phones(conn, client_id)
            # Потом добавляем новые
            for phone in phones:
                add_phone(conn, client_id, phone)


def delete_phone(conn, client_id, phone):
    with conn.cursor() as cursor:
        cursor.execute('''DELETE FROM phones 
                          WHERE client_id=%s AND number=%s''', (client_id, phone))
        conn.commit()


def delete_client(conn, client_id):
    with conn.cursor() as cursor:
        cursor.execute('''DELETE FROM clients WHERE id=%s''', (client_id,))
        conn.commit()


def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    with conn.cursor() as cursor:
        query = '''SELECT c.id, c.first_name, c.last_name, c.email, array_agg(p.number) AS phones
                   FROM clients c
                   LEFT JOIN phones p ON c.id = p.client_id
                   WHERE '''
        conditions = []
        values = []

        if first_name:
            conditions.append("c.first_name ILIKE %s")
            values.append('%{}%'.format(first_name))

        if last_name:
            conditions.append("c.last_name ILIKE %s")
            values.append('%{}%'.format(last_name))

        if email:
            conditions.append("c.email ILIKE %s")
            values.append('%{}%'.format(email))

        if phone:
            conditions.append("p.number ILIKE %s")
            values.append('%{}%'.format(phone))

        query += ' AND '.join(conditions) + ' GROUP BY c.id'

        cursor.execute(query, tuple(values))
        results = cursor.fetchall()

        return [{
            'id': result[0],
            'first_name': result[1],
            'last_name': result[2],
            'email': result[3],
            'phones': result[4]
        } for result in results]


def delete_phones(conn, client_id):
    with conn.cursor() as cursor:
        cursor.execute('''DELETE FROM phones WHERE client_id=%s''', (client_id,))
        conn.commit()


# Демонстрация работы всех функций
with psycopg2.connect(database="DB_HW3", user="postgres", password=input("Введите пароль от базы данных: ")) as conn:
    # Создание структуры базы данных
    create_db(conn)

    # Добавление клиента
    client_id = add_client(conn, 'Иван', 'Иванов', 'ivanov@example.com', ['+79161234567', '+79201234678'])
    print(f"Добавлен новый клиент с ID {client_id}")

    # Добавление дополнительного телефона
    add_phone(conn, client_id, '+79876543210')
    print("Добавлен дополнительный телефон")

    # Изменение данных клиента
    change_client(conn, client_id, first_name='Иван', last_name='Петров', email='petrov@example.com',
                  phones=['+79998887766'])
    print("Данные клиента изменены")

    # Удаление телефона
    delete_phone(conn, client_id, '+79998887766')
    print("Телефон удален")

    # Поиск клиента
    found_clients = find_client(conn, first_name='Иван', last_name='Петров')
    print("Найденные клиенты:")
    for client in found_clients:
        print(client)

    # Удаление клиента
    delete_client(conn, client_id)
    print("Клиент удален")

conn.close()