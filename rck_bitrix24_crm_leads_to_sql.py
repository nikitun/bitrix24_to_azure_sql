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
import datetime
import os.path

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
    number_string = number_string.replace(' ','')
    number_string = number_string.replace(',','.')
    try:
        return float(number_string)
    except ValueError:
        return None

# Функции для получения и обновления токенов

# Обновление пары ACCESS_TOKEN и REFRESH_TOKEN из REFRESH_TOKEN
def refresh_access_token(refresh_token):
    PAYLOAD =\
    {
        'grant_type':'refresh_token',
        'client_id':CLIENT_ID,
        'client_secret':CLIENT_SECRET,
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

print('rck_bitrix24_crm_leads_to_sql.py execution started')

try:
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+SERVER+';DATABASE='+DATABASE+';UID='+USERNAME+';PWD='+ PASSWORD)
except pyodbc.Error as err:
    print(err)
    time.sleep(3)
    sys.exit('execution interrupted')

cursor = cnxn.cursor()
cursor.fast_executemany = True

# # Удаление таблицы из базы данных
# cursor.execute('DROP TABLE [fact_tables].[leads]')
# cnxn.commit()

# # Очистка таблицы
# cursor.execute('TRUNCATE TABLE [fact_tables].[leads]')
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

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'leads')
BEGIN
/*Создание таблицы*/
CREATE TABLE [fact_tables].[leads]
(
    [ID лида]                     int,
    [ID этапа лида]               nvarchar(32),
    [Заголовок]                   nvarchar(256),
    [Дата создания]               nvarchar(32), /*datetime,*/
    [Дата закрытия]               nvarchar(32), /*datetime,*/
    [Запрашиваемая сумма кредита] money,
    [Сумма вознаграждения]        money,
    [ID контакта]                 int,
    [Имя контакта]                nvarchar(128),
    [Фамилия контакта]            nvarchar(128),
    [Отчество контакта]           nvarchar(128),
    [ID источника]                nvarchar(32),
    [ID сквозной нумерации]       nvarchar(32),
    [ID кредитного продукта]      int,
    [ID оператора]                int,
    [ID ответственного]           int
) ON [PRIMARY]
/*Поле-индекс*/
ALTER TABLE [fact_tables].[leads]
ADD CONSTRAINT RCK_fact_tables_leads_LeadID_Unique UNIQUE ([ID лида])
PRINT 'Table "leads" succesfully created'
END
ELSE
    PRINT 'Table "leads" already exists'
''')
cnxn.commit()

# Число строк и последнее значение id
last_total = table_rows_count(cursor, '[fact_tables].[leads]')
if last_total > 0:
    last_id = table_last_row_as_dict(cursor, '[fact_tables].[leads]', '[ID лида]')['ID лида']
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
    last_row_dict = table_last_row_as_dict(cursor, '[fact_tables].[leads]', '[ID лида]')
    last_row_date = datetime.datetime.strptime(last_row_dict['Дата создания'][0:10],'%Y-%m-%d').date()

    # Дата 60 дней назад от сегодня
    sixty_days_befor_date = datetime.datetime.today().date()-datetime.timedelta(DAYS_OFFSET)

    # Если дата последней строки больше, последняя строка удаляется из таблицы
    if last_row_date >= sixty_days_befor_date:
        cursor.execute('DELETE FROM [fact_tables].[leads] WHERE [ID лида] = ' + str(last_row_dict['ID лида']))
        last_total = last_total - 1
    else:
        last_id = last_row_dict['ID лида']
        break
else:
    last_id = 0
cnxn.commit()
print('last', DAYS_OFFSET, 'days deleted from table')
print('row count', last_total)
print('last row id', last_id)

# Чтение токенов из файла

# Если файл существует, они загружаются из файла, если нет, получаются из CODE
TOKENS_FILENAME = os.path.join(os.pardir, '../', 'rck_bitrix24_tokens.txt')
if os.path.isfile(TOKENS_FILENAME):
    ACCESS_TOKEN, REFRESH_TOKEN = read_tokens_from_txt_file(TOKENS_FILENAME)
else:
    sys.exit("REFRESH_TOKEN устарел, необходимо обновить файл с токенами")
    
# Чтобы выгрузить все поля "SELECT": [ "*", "UF_*" ]
# ASC - выдача элементов с начала списка, DESC - с конца
METHOD_NAME = 'crm.lead.list'
PAYLOAD =\
{
    'start': last_total
    ,'ORDER': { "ID": "ASC" } # ID лидов всегда возрастают
    ,'SELECT': [
        # Основная информация о лиде
        'ID'                    # ID лида
        ,'STATUS_ID'            # ID этапа лида
        ,'UF_CRM_1590830347'    # ID сквозной нумерации
        ,'TITLE'                # Название карточки лида
        ,'DATE_CREATE'          # Дата создания лида
        ,'DATE_CLOSED'          # Дата закрытия лида
        ,'OPPORTUNITY'          # Сумма вознаграждения
        # Информация о контакте
        ,'CONTACT_ID'           # ID контакта
        ,'NAME'                 # Имя контакта
        ,'SECOND_NAME'          # Фамилия контакта
        ,'LAST_NAME'            # Отчество контакта
        # Обязательные и дополнительные поля
        ,'UF_CRM_1580287308126' # ID кредитного продукта
        ,'UF_CRM_1560761054982' # Сумма кредита
        ,'SOURCE_ID'            # ID источника
        ,'UF_CRM_1566227400'    # ID оператора
        ,'ASSIGNED_BY_ID'       # ID ответственного
    ]
}

while True:
    # Во время цикла ACCESS_TOKEN может изменяться
    URL = 'https://'+INTERNET_NAME+'.bitrix24.ru/rest/'+METHOD_NAME+'/?auth='+ACCESS_TOKEN

    # Если "неудачный" статус, то обновление токена
    r = requests.post(url=URL, json=PAYLOAD)
    if r.status_code != 200:
        if r.status_code == 401:
            ACCESS_TOKEN, REFRESH_TOKEN = refresh_access_token(REFRESH_TOKEN)
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
                INSERT INTO [fact_tables].[leads]
                (
                    [ID лида],
                    [ID этапа лида],
                    [Заголовок],
                    [Дата создания],
                    [Дата закрытия],
                    [Запрашиваемая сумма кредита],
                    [Сумма вознаграждения],
                    [ID контакта],
                    [Имя контакта],
                    [Фамилия контакта],
                    [Отчество контакта],
                    [ID источника],
                    [ID сквозной нумерации],
                    [ID кредитного продукта],
                    [ID оператора],
                    [ID ответственного]
                )
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ''',\
                (\
                    none_if_not_data_str(e['ID']),
                    none_if_not_data_str(e['STATUS_ID']),
                    none_if_not_data_str(e['TITLE']),
                    none_if_not_data_str(e['DATE_CREATE']), #str_to_datetime_normalized(none_if_not_data_str(e['DATE_CREATE'])),
                    none_if_not_data_str(e['DATE_CLOSED']), #str_to_datetime_normalized(none_if_not_data_str(e['DATE_CLOSED'])),
                    str_to_money(none_if_not_data_str(e['UF_CRM_1560761054982'])),
                    str_to_money(none_if_not_data_str(e['OPPORTUNITY'])),
                    none_if_not_data_str(e['CONTACT_ID']),
                    none_if_not_data_str(e['NAME']),
                    none_if_not_data_str(e['SECOND_NAME']),
                    none_if_not_data_str(e['LAST_NAME']),
                    none_if_not_data_str(e['SOURCE_ID']),
                    none_if_not_data_str(e['UF_CRM_1590830347']),
                    none_if_not_data_str(e['UF_CRM_1580287308126']),
                    none_if_not_data_str(e['UF_CRM_1566227400']),
                    none_if_not_data_str(e['ASSIGNED_BY_ID'])
                )
            )
            cnxn.commit()

    toc = time.time()

    time.sleep(0 if (toc - tic) > 0.5 else 0.5 - (toc - tic))

    if 'next' in r.json().keys():
        PAYLOAD['start'] = r.json()['next']
        #display.clear_output(wait=True)
        print('loading in progress:', PAYLOAD['start'],'/',r.json()['total'])
    else:
        #display.clear_output(wait=True)
        print('loading finished:',r.json()['total'],'/',r.json()['total'])
        break

time.sleep(3)

# Отключение от базы данных
cursor = cnxn.cursor()  
cursor.close()
cnxn.close()