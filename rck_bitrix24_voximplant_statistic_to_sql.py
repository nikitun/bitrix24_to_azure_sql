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

print('rck_bitrix24_voximplant_statistic_to_sql.py execution started')

try:
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+SERVER+';DATABASE='+DATABASE+';UID='+USERNAME+';PWD='+ PASSWORD)
except pyodbc.Error as err:
    print(err)
    time.sleep(3)
    sys.exit('execution interrupted')

cursor = cnxn.cursor()
cursor.fast_executemany = True

# https://dev.1c-bitrix.ru/rest_help/scope_telephony/voximplant/statistic/voximplant_statistic_get.php
# https://dev.1c-bitrix.ru/rest_help/scope_telephony/codes_and_types.php

# # Удаление таблицы из базы данных
# cursor.execute('DROP TABLE [fact_tables].[voximplant_statistic]')
# cnxn.commit()

# # Очистка таблицы
# cursor.execute('TRUNCATE TABLE [fact_tables].[voximplant_statistic]')
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

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'voximplant_statistic')
BEGIN
    /*Создание таблицы*/
    CREATE TABLE [fact_tables].[voximplant_statistic]
    (
        [ID звонка]                   int,                        /* ID               Идентификатор звонка (для внутренних целей) */
        [Тип звонка]                  int,                        /* CALL_TYPE        Тип вызова (см. описание типов звонка) */
        [ID оператора]                int,                        /* PORTAL_USER_ID   Идентификатор ответившего оператора (если это тип звонка: 2 - Входящий) или идентификатор позвонившего оператора (если это тип звонка: 1 - Исходящий) */
        [Номер оператора]             nvarchar(32),               /* PORTAL_NUMBER    Номер, на который поступил звонок (если это тип звонка: 2 - Входящий) или номер, с которого был совершен звонок (1 - Исходящий) */
        [Номер абонента]              nvarchar(32),               /* PHONE_NUMBER     Номер, с которого звонит абонент (если это тип звонка: 2 - Входящий) или номер, которому звонит оператор (1 - Исходящий) */
        [Продолжительность звонка, с] int,                        /* CALL_DURATION    Продолжительность звонка в секундах */
        [Время начала звонка]         nvarchar(32), /*datetime,*/ /* CALL_START_DATE  Время инициализации звонка. При сортировке по этому полю нужно указывать дату в формате ISO-8601 */
        [Код вызова]                  nvarchar(8),                /* CALL_FAILED_CODE Код вызова (см. таблицу кодов вызова) */
        [Идентификатор дела CRM]      int,                        /* CRM_ACTIVITY_ID  Идентификатор дела CRM, созданного на основании звонка */
        [Идентификатор объекта CRM]   int,                        /* CRM_ENTITY_ID    Идентификатор объекта CRM, к которому прикреплено дело */
        [Тип объекта CRM]             nvarchar(32)                /* CRM_ENTITY_TYPE  Тип объекта CRM, к которому прикреплено дело, например: LEAD */
    ) ON [PRIMARY]
    /*Поле-индекс*/
    ALTER TABLE [fact_tables].[voximplant_statistic]
    ADD CONSTRAINT RCK_fact_tables_voximplant_statistic_CallID_Unique UNIQUE ([ID звонка])
    PRINT 'Table "voximplant_statistic" succesfully created'
END
ELSE
    PRINT 'Table "voximplant_statistic" already exists'
''')
cnxn.commit()

# Число строк и последнее значение id
last_total = table_rows_count(cursor, '[fact_tables].[voximplant_statistic]')
if last_total > 0:
    last_id = table_last_row_as_dict(cursor, '[fact_tables].[voximplant_statistic]', '[ID звонка]')['ID звонка']
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
    last_row_dict = table_last_row_as_dict(cursor, '[fact_tables].[voximplant_statistic]', '[ID звонка]')
    last_row_date = datetime.datetime.strptime(last_row_dict['Время начала звонка'][0:10],'%Y-%m-%d').date()

    # Дата 60 дней назад от сегодня
    sixty_days_befor_date = datetime.datetime.today().date()-datetime.timedelta(DAYS_OFFSET)

    # Если дата последней строки больше, последняя строка удаляется из таблицы
    if last_row_date >= sixty_days_befor_date:
        cursor.execute('DELETE FROM [fact_tables].[voximplant_statistic] WHERE [ID звонка] = ' + str(last_row_dict['ID звонка']))
        last_total = last_total - 1
    else:
        last_id = last_row_dict['ID звонка']
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
    sys.exit('REFRESH_TOKEN устарел, необходимо обновить файл с токенами')
    
# Чтобы выгрузить все поля 'SELECT': [ '*', 'UF_*' ]
# ASC - выдача элементов с начала списка, DESC - с конца
METHOD_NAME = 'voximplant.statistic.get'
PAYLOAD =\
{
    'start': last_total
    ,'ORDER': { 'ID': 'ASC' }   # ID, сортировка по возрастанию
    ,'SELECT': [
        'ID'                # ID звонка
        ,'CALL_TYPE'        # Тип звонка
        ,'PORTAL_USER_ID'   # ID оператора
        ,'PORTAL_NUMBER'    # Номер оператора
        ,'PHONE_NUMBER'     # Номер абонента
        ,'CALL_DURATION'    # Продолжительность звонка, с
        ,'CALL_START_DATE'  # Время начала звонка
        ,'CALL_FAILED_CODE' # Код вызова
        ,'CRM_ACTIVITY_ID'  # Идентификатор дела CRM
        ,'CRM_ENTITY_ID'    # Идентификатор объекта CRM
        ,'CRM_ENTITY_TYPE'  # Тип объекта CRM
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
                INSERT INTO [fact_tables].[voximplant_statistic]
                (
                    [ID звонка],
                    [Тип звонка],
                    [ID оператора],
                    [Номер оператора],
                    [Номер абонента],
                    [Продолжительность звонка, с],
                    [Время начала звонка],
                    [Код вызова],
                    [Идентификатор дела CRM],
                    [Идентификатор объекта CRM],
                    [Тип объекта CRM]
                )
                VALUES(?,?,?,?,?,?,?,?,?,?,?)
                ''',\
                (\
                    none_if_not_data_str(e['ID']),
                    none_if_not_data_str(e['CALL_TYPE']),
                    none_if_not_data_str(e['PORTAL_USER_ID']),
                    none_if_not_data_str(e['PORTAL_NUMBER']),
                    none_if_not_data_str(e['PHONE_NUMBER']),
                    none_if_not_data_str(e['CALL_DURATION']),
                    none_if_not_data_str(e['CALL_START_DATE']), #str_to_datetime_normalized(none_if_not_data_str(e['CALL_START_DATE'])),
                    none_if_not_data_str(e['CALL_FAILED_CODE']),
                    none_if_not_data_str(e['CRM_ACTIVITY_ID']),
                    none_if_not_data_str(e['CRM_ENTITY_ID']),
                    none_if_not_data_str(e['CRM_ENTITY_TYPE'])
                )
            )
            cnxn.commit()

    toc = time.time()

    time.sleep(0 if (toc - tic) > 0.5 else 0.5 - (toc - tic))

    #     break # Если нужно выгружить первые 50
    
    if 'next' in r.json().keys():
        PAYLOAD['start'] = r.json()['next']
        #display.clear_output(wait=True)
        print('loading in progress:', PAYLOAD['start'],'/',r.json()['total'])
    else:
        #display.clear_output(wait=True)
        print('loading finished:',r.json()['total'],'/',r.json()['total'])
        break

# Отключение от базы данных
cursor = cnxn.cursor()  
cursor.close()
cnxn.close()