import cx_Oracle
import calendar
import logging
import uuid
import time
import csv
import sys
import os
from smtplib import SMTP
from email.mime.text import MIMEText
from email.utils import formatdate
from email.mime.multipart import MIMEMultipart
from tabulate import tabulate
from datetime import datetime
from config import oracle_database, oracle_username, oracle_password, oracle_ip, full_upload, main_path, email_server, tkpb_email, tkpb_email_password, Database

errors = []
error_state = False

log_file_folder = main_path+fr'\{datetime.today().strftime("%Y.%m.%d")}'
isExist = os.path.exists(log_file_folder)
if not isExist:
    os.makedirs(log_file_folder)

logging.basicConfig(level=logging.INFO,
                    handlers=[
                        logging.StreamHandler(sys.stdout),
                        logging.FileHandler(log_file_folder+fr'\efrsb {datetime.today().strftime("%Y.%m.%d")}.log', mode="w"),
                    ],
                    format='%(asctime)s: %(levelname)s - %(message)s')

def is_report_in_db(doc_id, doc_type, bankrupt_id, upload_batch_guid):
    conn = None
    sql = None
    error_type = None
    error_message = None
    try:
        conn = Database() #connect_to_database(oracle_ip, oracle_database, oracle_username, oracle_password)
        cur = conn.connection.cursor()
        if doc_type == 'report':
            sql = 'select 1 from gis_efrsb_reports where ReportId = :myinput'
            error_type = 403
            error_message = f"Отчёт {doc_id} уже находится в таблице gis_efrsb_reports"
        if doc_type == 'message':
            sql = """select 1 from (select idmessage from gis_efrsb_messages
                  union
                  select idmessage from gis_efrsb_extr_bnkr) where idmessage = :myinput"""
            error_type = 204
            error_message = f"Сообщение {doc_id} уже находится в таблице gis_efrsb_messages или gis_efrsb_extr_bnkr"
        cur.execute(sql, myinput=doc_id)
        row = cur.fetchone()
        if row and row[0] == 1:
            error_handler('W', error_type, error_message, bankrupt_id, None, None, upload_batch_guid)
            return True
        else:
            return False
    except cx_Oracle.Error as error:
        error_handler('E', 912, 'Ошибка при попытке проверить существует ли данный отчёт в БД: ' + str(error), None, None, None, upload_batch_guid)


def is_debtor_in_db(bankruptid, upload_batch_guid):  # we check whether this debtor already exists in the db or not
    try:
        error_handler('I', 111, f'Проверка наличия должника в БД', bankruptid, None, None, upload_batch_guid)
        conn = Database() #connect_to_database(oracle_ip, oracle_database, oracle_username, oracle_password)
        cur = conn.connection.cursor()
        cur.execute("select 1 from gis_efrsb_debtors where bankruptid = :myinput", myinput=bankruptid)
        row = cur.fetchone()
        if row and row[0] == 1:
            error_handler('W', 103, f'Должник с ID {bankruptid} уже существует в БД', bankruptid, None, None, upload_batch_guid)
            return True
        else:
            return False
    except cx_Oracle.Error as error:
        error_handler('W', 906, 'Ошибка при попытке проверить существует ли данный должник в БД: ' + str(error), None, None, None, upload_batch_guid)

def get_upload_guid(conn, today):
    guid = str(uuid.uuid4())
    full_upload_value = 1 if full_upload is True else None
    statement = f"insert into gis_efrsb_uploads (id, uploaddate, fullupload ) values (:1, :2, :3)"

    try:
        with conn.cursor() as cursor:
            cursor.execute(statement, [guid, today, full_upload_value])
            conn.commit()
            error_handler('I', 110, f'Создан и сохранен глобальный ID загрузки {guid}', None,
                          None, None, guid)
            return guid
    except cx_Oracle.Error as error:
        error_handler('E', 903, 'Не удалось сохранить в БД глобальный идентификатор загрузки: ' + str(error), None, None, None, None)


def is_case_duplicate(current_cases, new_case, bankrupt_id, upload_batch_guid):
    duplicate_case = False
    for one_case in current_cases:
        if one_case['BankruptId'] == new_case['BankruptId'] and one_case['CaseNumber'] == new_case['CaseNumber']:
            duplicate_case = True
            error_handler('W', 101, f"Дело {new_case['CaseNumber']} банкрота {bankrupt_id} уже есть в данной сессии", bankrupt_id, None, None, upload_batch_guid)
            break
    return duplicate_case

def execute_many(cursor, statement, data, table_name, upload_batch_guid):
    cursor.executemany(statement, data, batcherrors=True)
    error_id = None
    if table_name in ['gis_efrsb_debtors', 'gis_efrsb_names', 'gis_efrsb_cases']:
        error_id = 'BankruptId'
    elif table_name == 'gis_efrsb_reports':
        error_id = 'ReportId'
    elif table_name in ['gis_efrsb_messages', 'gis_efrsb_extr_bnkr']:
        error_id = 'IdMessage'
    else:
        error_id = None
    duplicate_count = 0
    for error in cursor.getbatcherrors():
        error_text = None
        if error.code == 1:
            duplicate_count += 1
            if table_name == 'gis_efrsb_debtors' and full_upload == False:
                error_handler('I', 915,
                            f"Сверка данных для дублируемого банкрота {duplicate_count}",
                            data[error.offset][error_id], None, None, upload_batch_guid)
                try:
                    return_val = cursor.callfunc("xxi.gis_efrsb_util.check_duplicate", str, [data[error.offset][error_id], data[error.offset]['lastname'], data[error.offset]['firstname'], data[error.offset]['patronymicname'], data[error.offset]['fullname'], data[error.offset]['inn'], data[error.offset]['snils'], data[error.offset]['ogrn'], data[error.offset]['region'], data[error.offset]['address'], data[error.offset]['birthdate'], data[error.offset]['birthplace']])
                    if return_val == 'Success':
                        error_handler('W', 915,
                                    f"Данные для дублируемого банкрота обновлены",
                                    data[error.offset][error_id], None, None, upload_batch_guid)
                    elif return_val == 'No changes':
                        error_handler('I', 915,
                                    f"Изменений нет",
                                    data[error.offset][error_id], None, None, upload_batch_guid)
                    else:
                        error_handler('E', 901,
                                    f"Ошибка сверки данных дублируемого банкрота {return_val}",
                                    data[error.offset][error_id], None, None, upload_batch_guid)
                except cx_Oracle.Error as er:
                    error_handler('E', 903, 'Ошибка сверки данных дублируемого банкрота: ' + str(er), data[error.offset][error_id], None, None, upload_batch_guid)
        else:
            if error_id:
                error_text = f'Идентификатор {data[error.offset][error_id]}'
            else:
                error_text = f'Исходная строка {data[error.offset]}'
            error_handler('E', 901,
                          f"БД: ошибка загрузки в {table_name}: \n{error.message}. \n{error_text}",
                          None, None, None, upload_batch_guid)
    if duplicate_count:
        error_handler('E', 901,
                      f"БД: {duplicate_count} дубликат(ов) не загружено в {table_name}",
                      None, None, None, upload_batch_guid)


def upload_to_db(data, table_name, upload_batch_guid):
    error_handler('I', 915,
                  f"Начало загрузки в БД",
                  None, None, None, upload_batch_guid)
    cols = ','.join(list(data[0].keys()))
    params = ','.join(':' + str(k) for k in list(data[0].keys()))
    statement = f"insert into {table_name} ({cols}) values ({params})"

    try:
        conn = Database() #connect_to_database(oracle_ip, oracle_database, oracle_username, oracle_password)
        with conn.connection.cursor() as cursor:
            try:
                #разбиваем данные на 5 тыс записей, чтобы избежать ошибки DPI-1015
                start_pos = 0
                batch_size = 5000
                error_handler('I', 915,
                  f"Размер списка {len(data)}, размер одной пачки загрузки {batch_size}",
                  None, None, None, upload_batch_guid)
                if len(data) <= batch_size:
                    execute_many(cursor, statement, data, table_name, upload_batch_guid)
                else:
                    while start_pos < len(data):
                        error_handler('I', 900, f"БД: Загрузка с {start_pos} по {start_pos+batch_size} записей в {table_name}", None, None, None, upload_batch_guid)
                        data_batch = data[start_pos:start_pos + batch_size]
                        start_pos += batch_size
                        execute_many(cursor, statement, data_batch, table_name, upload_batch_guid)

                conn.connection.commit()
                error_handler('I', 915,
                              f"Успешная загрузка в БД",
                              None, None, None, upload_batch_guid)

            except cx_Oracle.Error as e:
                error_handler('E', 901, 'Ошибка загрузки в БД ' + str(e), None, None, None, upload_batch_guid)
    except cx_Oracle.Error as error:
        error_handler('E', 902, 'Ошибка подключения к БД: ' + str(error), None, None, None, upload_batch_guid)

def saveascsv(data, file_path, upload_batch_guid):
    try:
        headers = list(data[0].keys())
        with open(file_path, 'w', encoding='cp1251', newline='', errors='replace') as f:
            writer = csv.DictWriter(f, fieldnames=headers, delimiter="^")
            writer.writeheader()
            writer.writerows(data)
    except Exception as e:
        error_handler('E', 904, 'Ошибка сохранения в csv-файл ' + str(e), None, None, None, upload_batch_guid)
        raise


def finish_session(upload_batch_guid):
    try:
        conn = Database() #connect_to_database(oracle_ip, oracle_database, oracle_username, oracle_password)
        statement = f"update gis_efrsb_uploads set finished = 1 where id = :1"
        with conn.connection.cursor() as cursor:
            try:
                cursor.execute(statement, [upload_batch_guid])
                conn.connection.commit()
            except cx_Oracle.Error as e:
                error_handler('E', 905, f'Не удалось пометить загрузку {upload_batch_guid} как завершенная' + str(e), None, None, None, upload_batch_guid)
    except cx_Oracle.Error as error:
        error_handler('E',902, 'Ошибка подключения к БД: ' + str(error), None, None, None, upload_batch_guid)

def save_data(data, upload_batch_guid):  # save data to csv files and the database
    for my_list in data:
        error_handler('I', 917, f"Файл {my_list['title'].upper()}", None, None, None, upload_batch_guid)
        if my_list['data']:
            error_handler('I', 917, f"Сохранение данных в {my_list['title'].upper()}.csv файл", None, None, None, upload_batch_guid)
            saveascsv(my_list['data'], log_file_folder+f"\{my_list['title']} - {upload_batch_guid}.csv", upload_batch_guid)
            upload_to_db(my_list['data'], my_list['table'], upload_batch_guid)
        else:
            error_handler('W', 918, f"{my_list['title']} пуст. Нет данных для сохранения", None, None, None, upload_batch_guid)
        logger_new_line()

def get_date_range(today, upload_batch_guid):
    start_date = None

    if full_upload is True:
        years = []
        last_year = int(today.year)
        start_year = 2011
        while start_year <= last_year:
            years.append(start_year)
            start_year += 1

        months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        result = []

        for year in years:
            for month in months:
                start_date = datetime(year, month, 1)
                res = calendar.monthrange(year, month)
                last_day = res[1]
                end_date = datetime(year, month, last_day, hour=23, minute=59, second=59, microsecond=999999)
                my_tuple = (start_date, end_date)
                result.append(my_tuple)
    else:
        #conn = None
        result = []
        try:
            error_handler('I', 900,
                  f"Дельта: получение даты последней загрузки из БД",  None, None, None, None)
            conn = Database() #connect_to_database(oracle_ip, oracle_database, oracle_username, oracle_password)
            cur = conn.connection.cursor()
            for row in cur.execute("""SELECT UPLOADDATE FROM gis_efrsb_uploads
                                  WHERE UPLOADDATE = (SELECT MAX(uploaddate)
                                                      FROM gis_efrsb_uploads
                                                      WHERE finished = 1)"""):
                latest_upload = row[0]
                #latest_upload = datetime.strptime('2023-12-18T11:16:11Z', '%Y-%m-%dT%H:%M:%SZ')
                if abs((today - latest_upload).days) > 31:
                    error_handler('E', 102, f"Дата последней успешной загрузки превышает 31 день: разница = {abs((today - latest_upload).days)}. Должно быть менее данного значения", None, None, None, upload_batch_guid)
                else:
                    start_date = latest_upload
                    #newtoday=datetime.strptime('2023-12-18T11:16:11Z', '%Y-%m-%dT%H:%M:%SZ')
                    my_tuple = (latest_upload, today)
                    result.append(my_tuple)
        except cx_Oracle.Error as error:
            error_handler('E', 903, 'Ошибка подключения к БД при попытке получить последнюю дату загрузки: ' + str(error), None, None, None, upload_batch_guid)

    return result, start_date


def error_handler(error_type, error_code, error_text, id_debtor, id_message, message_type, upload_batch_id):
    # ERROR_TYPE should be I (Info), W (Warning), E (Error)
    error = {'Type': error_type,
             'Code': error_code,
             'Text': error_text,
             'BankruptId': id_debtor,
             'IdMessage': id_message,
             'MessageType': message_type,
             'uploadBatchId': upload_batch_id}

    log_message = str(error_code)+' '+error_text
    if id_debtor:
        log_message += '. Банкрот #: '+ str(id_debtor)
    if id_message:
        log_message += '. Сообщение #: ' + str(id_message)
    if message_type:
        log_message += '. Тип сообщения: ' + message_type

    if error_type == 'I':
        logging.info(log_message)
    elif error_type == 'W':
        logging.warning(log_message)
    else:
        logging.error(log_message)
    print()

    global error_state
    if error_code in [902, 911] and error_state is False:
        error_state = True
    if error_type in ['E', 'W']:
        errors.append(error)

def logger_new_line():
    logging.info('<------------------------------------------------------------------------------------->\n')

def check_finish_session(upload_batch_id):
    if error_state is False:
        finish_session(upload_batch_id)

def send_email(recipients, subject, content):
    msg = MIMEMultipart()
    msg['from'] = 'hub@tkpb.ru'
    msg['to'] = ", ".join(recipients)
    msg['Date'] = formatdate()
    msg['subject'] = subject
    text = MIMEText(content, 'plain')
    msg.attach(text)
    print('\n')
    try:
        server = SMTP()
        server.connect(email_server)
        server.login(tkpb_email, tkpb_email_password)
        server.sendmail(msg['from'], recipients, msg.as_string())
        print(f"Уведомление отправлено на {recipients}")
    except Exception as e:
        print(f"При отправке письма произошла ошибка: {e}")

def error_report():
    send_email(['k_popov@tkpb.ru', 'kka@tkpb.ru'], 'ЕФРСБ Банкроты', f'Скрипт завершил работу. Проверьте лог в: {log_file_folder}')
    error_handler('I', 900,
                  f"Сводка ошибок и предупреждений данной загрузки: \n {tabulate(errors, headers='keys', tablefmt='psql')}",
                  None, None,
                  None, None)


