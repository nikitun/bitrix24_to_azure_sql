# -*- coding: utf-8 -*-

################################################################################

# Подключение к базе данных
import pyodbc 
import time
import sys

SERVER   = '' 
DATABASE = '' 
USERNAME = '' 
PASSWORD = ''

# Считывание данных из Битрикс24 и загрузка их в базу данных
import requests
import pandas as pd
import json
import xlsxwriter
import os.path
import pytz

INTERNET_NAME = ''
CLIENT_ID     = ''
CLIENT_SECRET = ''

################################################################################

# Функции для работы с БД

def table_rows_count(pyodbc_cursor, table_name):
    pyodbc_cursor.execute('SELECT COUNT(*) FROM '+ table_name)
    table_rows_count = pyodbc_cursor.fetchone()[0]
    return table_rows_count

def table_last_row_as_row(pyodbc_cursor, table_name, column_sort_name):
    pyodbc_cursor.execute('SELECT TOP 1 * FROM ' + table_name + ' ORDER BY ' + column_sort_name + ' DESC')
    last_row_as_row = pyodbc_cursor.fetchone() # pyodbc.Row
    return last_row_as_row

def table_last_row_as_dict(pyodbc_cursor, table_name, column_sort_name):
    pyodbc_cursor.execute('SELECT TOP 1 * FROM ' + table_name + ' ORDER BY ' + column_sort_name + ' DESC')
    last_row_as_row    = pyodbc_cursor.fetchone()
    columns_names_list = [column[0] for column in pyodbc_cursor.description]
    last_row_as_dict   = {k:last_row_as_row[i] for i,k in enumerate(columns_names_list)}
    return last_row_as_dict

def none_if_not_data_str(input_value):
    if type(input_value) is not str:
        return None
    if input_value.isspace():
        return None
    return input_value

def str_to_money(number_string):
    if not number_string:
        return None
    number_string = number_string.replace(' ', '')
    number_string = number_string.replace(',', '.')
    try:
        return float(number_string)
    except ValueError:
        return None

# Функции для получения и обновления токенов

# Обновление пары ACCESS_TOKEN и REFRESH_TOKEN из REFRESH_TOKEN
def refresh_access_token(client_id, client_secret, refresh_token):
    PAYLOAD =\
    {
        'grant_type':'refresh_token',
        'client_id':client_id,
        'client_secret':client_secret,
        'refresh_token':refresh_token
    }
    r = requests.get('https://oauth.bitrix.info/oauth/token', params = PAYLOAD)
    if r.status_code != 200:
        print (r.text)
        return '-1', '-1'
    access_token  = r.json()['access_token']
    refresh_token = r.json()['refresh_token']
    return access_token, refresh_token  # Строки

# Запись пары ACCESS_TOKEN и REFRESH_TOKEN в файл
def save_tokens_to_txt_file(access_token, refresh_token, filename):
    with open(filename, 'w') as tokens_file:
        tokens_file.write(access_token + '\n')
        tokens_file.write(refresh_token)

# Чтение пары ACCESS_TOKEN и REFRESH_TOKEN из файла
def read_tokens_from_txt_file(filename):
    with open(filename, 'r') as tokens_file:
        access_token = tokens_file.readline()[:-1]
        refresh_token = tokens_file.readline()
        return access_token, refresh_token

################################################################################

print('rck_bitrix24_crm_deals_to_sql.py execution started')

try:
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+SERVER+';DATABASE='+DATABASE+';UID='+USERNAME+';PWD='+ PASSWORD)
except pyodbc.Error as err:
    print(err)
    time.sleep(3)
    sys.exit('execution interrupted')

cursor = cnxn.cursor()
cursor.fast_executemany = True

# # Удаление таблицы из базы данных
# cursor.execute('DROP TABLE [fact_tables].[deals]')
# cnxn.commit()

# # Очистка таблицы
# cursor.execute('TRUNCATE TABLE [fact_tables].[deals]')
# cnxn.commit()

################################################################################

# Создание схемы и таблицы, если они не существуют
cursor.execute(\
'''
USE RCK

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = N'fact_tables')
BEGIN
    EXEC ('CREATE SCHEMA [fact_tables] AUTHORIZATION [dbo]')
    PRINT 'Schema "fact_tables" succesfully created'
END
ELSE
    PRINT 'Schema "fact_tables" already exists'

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'deals')
BEGIN
    /*Создание таблицы*/
    CREATE TABLE [fact_tables].[deals]
    (
        [ID сделки]                    int,
        [Заголовок]                    nvarchar(256),
        [Дата создания сделки]         nvarchar(32), /*datetime,*/
        [Дата начала сделки]           nvarchar(32), /*datetime,*/
        [Дата завершения сделки]       nvarchar(32), /*datetime,*/
        [Дата заполнения встречи]      nvarchar(32), /*datetime,*/
        [Повторная сделка]             char(1),       /*"Y"/"N"*/
        [ID этапа сделки]              nvarchar(32),
        [ID контакта]                  int,
        [ID источника]                 nvarchar(32),
        [ID сквозной нумерации]        nvarchar(32),  /*Содержит значения типа DL1811*/
        [Запрашиваемая сумма кредита]  money,
        [Сумма выданного кредита]      money,
        [Сумма вознаграждения]         money,
        [Вознаграждение (%)]           nvarchar(256),
        [Итоги встречи]                nvarchar(256), /* Потом это будет комментарий */
        [Количество встреч с клиентом] int,
        [Статус встречи с клиентом]    nvarchar(32),  /* Добавлено 2020.08 */
        [ID кредитного продукта]       int,
        [ID оператора]                 int,
        [ID менеджера]                 int,
        [ID ответственного]            int
    ) ON [PRIMARY]
    /*Поле-индекс*/
    ALTER TABLE [fact_tables].[deals]
    ADD CONSTRAINT RCK_fact_tables_deals_DealID_Unique UNIQUE ([ID сделки])
    PRINT 'Table "deals" succesfully created'
END
ELSE
    PRINT 'Table "deals" already exists'
''')
cnxn.commit()

# Число строк и последнее значение id
last_total = table_rows_count(cursor, '[fact_tables].[deals]')
if last_total > 0:
    last_id = table_last_row_as_dict(cursor, '[fact_tables].[deals]', '[ID сделки]')['ID сделки']
else:
    last_id = 0
print('row count', last_total)
print('last row id', last_id)

# Удаление из таблицы последних 60 дней
import datetime
DAYS_OFFSET = 60
print('deleting last', DAYS_OFFSET, 'days from table')
while last_total:

    # Дата из последней строки, с каждой итерацией дата будет меняться, т.к последняя строка удаляется
    last_row_dict = table_last_row_as_dict(cursor, '[fact_tables].[deals]', '[ID сделки]')
    last_row_date = datetime.datetime.strptime(last_row_dict['Дата создания сделки'][0:10],'%Y-%m-%d').date()
    
    # Дата 60 дней назад от сегодня
    sixty_days_befor_date = datetime.datetime.today().date()-datetime.timedelta(DAYS_OFFSET)

    # Если дата последней строки больше, последняя строка удаляется из таблицы
    if last_row_date >= sixty_days_befor_date:
        cursor.execute('DELETE FROM [fact_tables].[deals] WHERE [ID сделки] = ' + str(last_row_dict['ID сделки']))
        last_total = last_total - 1
    else:
        last_id = last_row_dict['ID сделки']
        break
else:
    last_id = 0
cnxn.commit()
print('last', DAYS_OFFSET, 'days deleted from table')
print('table row count', last_total)
print('last row id', last_id)

# Чтение токенов из файла

# Если файл существует, они загружаются из файла, если нет, получаются из CODE
TOKENS_FILENAME = os.path.join(os.pardir, '../', 'rck_bitrix24_tokens.txt')
if os.path.isfile(TOKENS_FILENAME):
    ACCESS_TOKEN, REFRESH_TOKEN = read_tokens_from_txt_file(TOKENS_FILENAME)
else:
    sys.exit("REFRESH_TOKEN устарел, необходимо обновить файл с токенами")
    
# Чтобы выгрузить все поля 'SELECT': [ '*', 'UF_*' ]
# ASC - выдача элементов с начала списка, DESC - с конца
METHOD_NAME = 'crm.deal.list'
PAYLOAD =\
{
    'start': last_total
    ,'ORDER': { 'ID': 'ASC' } # ID лидов всегда возрастают
    ,'SELECT': [
        # Основная информация о лиде
        'ID'                      # ID сделки
        ,'STAGE_ID'               # ID этапа сделки
        ,'UF_CRM_5ED225D67B474'   # ID сквозной нумерации
        ,'TITLE'                  # Название карточки сделки
        ,'DATE_CREATE'            # Дата создания сделки
        ,'BEGINDATE'              # Дата начала сделки
        ,'CLOSEDATE'              # Дата завершения сделки
        ,'OPPORTUNITY'            # Сумма вознаграждения
        ,'UF_CRM_5D4D5B9C830E2'   # Запрашиваемая сумма кредита
        ,'UF_CRM_1594562907'      # Сумма выданного кредита
        ,'UF_CRM_1559715164825'   # Вознаграждение (%)
        ,'UF_CRM_1589558853612'   # Дата заполнения встречи
        ,'UF_CRM_1589558190911'   # Итоги встречи
        ,'UF_CRM_1589559456'      # Количество встреч с клиентом
        ,'UF_CRM_1596380010602'   # Статус встречи с клиентом
        # Информация о контакте
        ,'CONTACT_ID'             # ID контакта
        ,'IS_RETURN_CUSTOMER'     # Повторная сделка
        # Обязательные и дополнительные поля
        ,'UF_CRM_5E31461BD65EC'   # ID кредитного продукта
        ,'SOURCE_ID'              # ID источника
        ,'UF_CRM_5D5ABFF2682FD'   # ID оператора
        ,'UF_CRM_1568217110'      # ID менеджера
        ,'ASSIGNED_BY_ID'         # ID ответственного
    ]
}

while True:
    # Во время цикла ACCESS_TOKEN может изменяться
    URL = 'https://'+INTERNET_NAME+'.bitrix24.ru/rest/'+METHOD_NAME+'/?auth='+ACCESS_TOKEN

    # Если "неудачный" статус, то обновление токена
    r = requests.post(url=URL, json=PAYLOAD)
    if r.status_code != 200:
        if r.status_code == 401:
            ACCESS_TOKEN, REFRESH_TOKEN = refresh_access_token(CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN)
            save_tokens_to_txt_file(ACCESS_TOKEN, REFRESH_TOKEN, TOKENS_FILENAME)
            continue
        else:
            print(r.text)
            break

    # Добавление выгрузки в выходной список
    output = r.json()['result']
    
    tic = time.time()
    
    for e in output:
        if int(e['ID']) > last_id:
            cursor.execute\
            (\
                '''
                INSERT INTO [fact_tables].[deals]
                (
                    [ID сделки],
                    [Заголовок],
                    [Дата создания сделки],
                    [Дата начала сделки],
                    [Дата завершения сделки],
                    [Дата заполнения встречи],
                    [Повторная сделка],
                    [ID этапа сделки],
                    [ID контакта],
                    [ID источника],
                    [ID сквозной нумерации],
                    [Запрашиваемая сумма кредита],
                    [Сумма выданного кредита],
                    [Сумма вознаграждения],
                    [Вознаграждение (%)],
                    [Итоги встречи],
                    [Количество встреч с клиентом],
                    [Статус встречи с клиентом],
                    [ID кредитного продукта],
                    [ID оператора],
                    [ID менеджера],
                    [ID ответственного]
                )
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ''',\
                (\
                    none_if_not_data_str(e['ID']),
                    none_if_not_data_str(e['TITLE']),
                    none_if_not_data_str(e['DATE_CREATE']),          #str_to_datetime_normalized(none_if_not_data_str(e['DATE_CREATE'])),
                    none_if_not_data_str(e['BEGINDATE']),            #str_to_datetime_normalized(none_if_not_data_str(e['BEGINDATE'])),
                    none_if_not_data_str(e['CLOSEDATE']),            #str_to_datetime_normalized(none_if_not_data_str(e['CLOSEDATE'])),
                    none_if_not_data_str(e['UF_CRM_1589558853612']), #str_to_datetime_normalized(none_if_not_data_str(e['UF_CRM_1589558853612'])),
                    none_if_not_data_str(e['IS_RETURN_CUSTOMER']),
                    none_if_not_data_str(e['STAGE_ID']),
                    none_if_not_data_str(e['CONTACT_ID']),
                    none_if_not_data_str(e['SOURCE_ID']),
                    none_if_not_data_str(e['UF_CRM_5ED225D67B474']),
                    str_to_money(none_if_not_data_str(e['UF_CRM_5D4D5B9C830E2'])),
                    str_to_money(none_if_not_data_str(e['UF_CRM_1594562907'])),
                    str_to_money(none_if_not_data_str(e['OPPORTUNITY'])),
                    none_if_not_data_str(e['UF_CRM_1559715164825']),
                    none_if_not_data_str(e['UF_CRM_1589558190911']),
                    none_if_not_data_str(e['UF_CRM_1589559456']),
                    none_if_not_data_str(e['UF_CRM_1596380010602']),
                    none_if_not_data_str(e['UF_CRM_5E31461BD65EC']),
                    none_if_not_data_str(e['UF_CRM_5D5ABFF2682FD']),
                    none_if_not_data_str(e['UF_CRM_1568217110']),
                    none_if_not_data_str(e['ASSIGNED_BY_ID'])
                )
            )
            cnxn.commit()

    toc = time.time()

    time.sleep(0 if (toc - tic) > 0.5 else 0.5 - (toc - tic))

    #break # Если нужно выгружить первые 50
    
    if 'next' in r.json().keys():
        PAYLOAD['start'] = r.json()['next']
        print('loading in progress:', PAYLOAD['start'],'/',r.json()['total'])
    else:
        print('loading finished:',r.json()['total'],'/',r.json()['total'])
        break

time.sleep(3)

# Отключение от базы данных
cursor = cnxn.cursor()  
cursor.close()
cnxn.close()