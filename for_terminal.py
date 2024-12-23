# Исходные данные для заполнения таблиц
import csv

with open('customers_data.csv', newline='') as file:
    csv_reader = csv.reader(file)
    header_1 = next(csv_reader)
    customers_data = [row for row in csv.reader(file) if 'customer_id' not in row]

with open('employees_data.csv', newline='') as file:
    csv_reader = csv.reader(file)
    header_2 = next(csv_reader)
    employees_data = [row for row in csv.reader(file) if 'employee_id' not in row]

with open('orders_data.csv', newline='') as file:
    csv_reader = csv.reader(file)
    header_3 = next(csv_reader)
    orders_data = [row for row in csv.reader(file) if 'order_id' not in row]

# Импортируйте библиотеку psycopg2

import psycopg2

# Создайте подключение к базе данных
conn = psycopg2.connect(
        host='sql_db',
        port='5432',
        database='analysis',
        user='simple',
        password='qweasd963'
    )

# Открытие курсора
cur = conn.cursor()

# Не меняйте и не удаляйте эти строки - они нужны для проверки
cur.execute("create schema if not exists itresume13315;")
cur.execute("DROP TABLE IF EXISTS itresume13315.orders")
cur.execute("DROP TABLE IF EXISTS itresume13315.customers")
cur.execute("DROP TABLE IF EXISTS itresume13315.employees")


# Ниже напишите код запросов для создания таблиц
cur.execute('''CREATE TABLE IF NOT EXISTS itresume13315.customers (
customer_id char(5) PRIMARY KEY NOT NULL,
 company_name VARCHAR(100) NOT NULL,
 contact_name VARCHAR(100) NOT NULL
 ); ''')
cur.execute('''
CREATE TABLE IF NOT EXISTS itresume13315.employees (
employee_id SERIAL PRIMARY KEY NOT NULL,
first_name VARCHAR(25) NOT NULL,
last_name VARCHAR(35) NOT NULL,
title VARCHAR(100) NOT NULL,
birth_date DATE NOT NULL,
notes TEXT
);
''')

cur.execute('''CREATE TABLE IF NOT EXISTS itresume13315.orders (
order_id INT PRIMARY KEY NOT NULL,
customer_id char(5) NOT NULL,
employee_id INT NOT NULL,
order_date DATE NOT NULL,
ship_city VARCHAR(100) NOT NULL,
FOREIGN KEY (customer_id) REFERENCES itresume13315.customers(customer_id),
FOREIGN KEY (employee_id) REFERENCES itresume13315.employees(employee_id)
); ''')


# Зафиксируйте изменения в базе данных
conn.commit()

#Теперь приступаем к операциям вставок данных
# Запустите цикл по списку customers_data и выполните запрос формата
# INSERT INTO itresume3270.table (column1, column2, ...) VALUES (%s, %s, ...) returning ", data)
# В конце каждого INSERT-запроса обязательно должен быть оператор returning
for row in customers_data:
        query = f"INSERT INTO itresume13315.customers ({', '.join(header_1)}) VALUES ({', '.join(['%s'] * len(row))}) RETURNING {', '.join(header_1)}"
        cur.execute (query, row)


# Не меняйте и не удаляйте эти строки - они нужны для проверки
conn.commit()
res_customers = cur.fetchall()

# Запустите цикл по списку employees_data и выполните запрос формата
# INSERT INTO itresume13315.table (column1, column2, ...) VALUES (%s, %s, ...) returning *", data)
# В конце каждого INSERT-запроса обязательно должен быть оператор returning *
res_employees = []  # Список для хранения вставленных данных
for row in employees_data:
    query = f"""
        INSERT INTO itresume13315.employees ({', '.join(header_2)})
        VALUES ({', '.join(['%s'] * len(row))})
        RETURNING employee_id, {', '.join(header_2)};
    """
    cur.execute(query, row)  # Выполняем запрос
    inserted_row = cur.fetchone()  # Получаем вставленную строку
    res_employees.append(inserted_row)  # Сохраняем результат

# Не меняйте и не удаляйте эти строки - они нужны для проверки
conn.commit()  # Зафиксируйте изменения
res_employees = cur.fetchall()
#print(res_employees)

# Запустите цикл по списку orders_data и выполните запрос формата
# INSERT INTO itresume13315.table (column1, column2, ...) VALUES (%s, %s, ...) returning *", data)
# В конце каждого INSERT-запроса обязательно должен быть оператор returning *
for row in orders_data:
        query = f"""
    INSERT INTO itresume13315.orders ({', '.join(header_3)}) 
    VALUES ({', '.join(['%s'] * len(row))}) 
    RETURNING {', '.join(header_3)};
    """
        inserted_row = cur.fetchone()  # Получаем вставленную строку
        #print(f"Inserted: {inserted_row}")  # Выводим результат
        cur.execute (query, row)


# Не меняйте и не удаляйте эти строки - они нужны для проверки
conn.commit()
res_orders = cur.fetchall()

# Закрытие курсора
cur.close()

# Закрытие соединения
conn.close()