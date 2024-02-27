from config import webServiceUrl, username, password,  Database #oracle_database, oracle_username, oracle_password, oracle_ip
from service_methods import connect_to_efrsb, get_registry_by_period
from data_processing import get_debtor_reports, get_debtor_messages, debtor_parser, name_history, legal_cases
from utils import error_handler, get_upload_guid, save_data, check_finish_session, get_date_range, logger_new_line, error_report 
import requests.auth
from zeep import Client, helpers, Settings
from requests import Session
from zeep.transports import Transport
from datetime import datetime  # , timedelta
from tabulate import tabulate
import os
import time

# Creating headers for the service connection
session = Session()
settings = Settings(xml_huge_tree=True)
session.auth = requests.auth.HTTPDigestAuth(username, password)

today = datetime.today()  # - timedelta(days=2093) in case we need to test dates from the past
datetime_str_start = '2023-05-01T00:00:00Z'
datetime_str_end = '2023-05-31T23:59:59Z'

debtors = []
reports = []
messages = []
messages_ext = []  
conn = Database()

upload_batch_guid = get_upload_guid(conn.connection, today)
client = connect_to_efrsb(session, Client, webServiceUrl, Transport, upload_batch_guid, settings)


def get_registry():
    daterange, start_date = get_date_range(today, upload_batch_guid)
    error_handler('I', 109,
                  f"Диапазон дат, по которым будут направлены запросы: \n {tabulate(daterange, headers=['Start Date', 'End Date'], tablefmt='psql')}",
                  None, None,
                  None, upload_batch_guid)
    logger_new_line()
    for month in daterange:
        if today > month[0]:  # avoid executing the script if the date in the loop goes beyond the current date
            resp = get_registry_by_period(client, month[0], month[1], upload_batch_guid)
            if resp is not None:
                debtors_dict = helpers.serialize_object(resp, dict)  # creating a dictionary
                for values in debtors_dict:
                    number_of_debtors = len(debtors_dict[values])
                    error_handler('I', 100, f"Получено банкротов: {number_of_debtors}", None, None, None, upload_batch_guid)
                    logger_new_line()
                    time.sleep(2)
                    debtor_counter = 0
                    for value in debtors_dict[values]:
                        for key in value:
                            error_handler('I', 100,
                                          f"Банкрот {debtor_counter+1} из {number_of_debtors}",
                                          None, None,
                                          None, upload_batch_guid)
                            debtor, bankrupt_id = debtor_parser(value[key], key, upload_batch_guid)
                            duplicate = False
                            for one in debtors:  # check if the debtor already exists in the list of the debtors received within this session
                                if one['BankruptId'] == bankrupt_id:
                                    duplicate = True
                                    error_handler('E',101, 'В периоде ' + str(month) + ' получен банкрот с существующим ID', bankrupt_id, 'GetDebtorsByLastPublicationPeriod', None, upload_batch_guid)
                                    break
                            if duplicate is False:
                                debtors.append(debtor)
                                if key != 'DebtorCompany':  # reports and messages are not collected for companies because if they are in the registry the bank doesn't want to do anything with them
                                    debtor_reports = get_debtor_reports(client, bankrupt_id, upload_batch_guid, start_date)
                                    debtor_messages, debtor_ext_msg = get_debtor_messages(client, bankrupt_id, upload_batch_guid, start_date)
                                    logger_new_line()
                                    if debtor_reports:
                                        reports.extend(debtor_reports)
                                    if debtor_messages:
                                        messages.extend(debtor_messages)
                                    if debtor_ext_msg:
                                        messages_ext.extend(debtor_ext_msg)
                            debtor_counter += 1
            else:
                error_handler('W', 107, f"За период {month[0]} - {month[1]} получено ноль банкротов", None, None, None, upload_batch_guid)
                logger_new_line()
        else:
            error_handler('I', 108, f"Текущая дата конца диапазона цикла {month[0].strftime('%Y-%m-%d %H:%M:%S')} превышает сегодняшнюю дату {today.strftime('%Y-%m-%d %H:%M:%S')}. Запросы после сегодняшней даты не будут отправлены", None, None,
                          None, upload_batch_guid)
            break

    efrsb = [{'title': 'Debtors', 'data': debtors, 'table': 'gis_efrsb_debtors'},
             {'title': 'Name History', 'data': name_history, 'table': 'gis_efrsb_names'},
             {'title': 'Legal Cases', 'data': legal_cases, 'table': 'gis_efrsb_cases'},
             {'title': 'Reports', 'data': reports, 'table': 'gis_efrsb_reports'},
             {'title': 'Messages', 'data': messages, 'table': 'gis_efrsb_messages'},
             {'title': 'Extrajudicial Messages', 'data': messages_ext, 'table': 'gis_efrsb_extr_bnkr'}]

    save_data(efrsb, upload_batch_guid)
    check_finish_session(upload_batch_guid)

if __name__ == "__main__":
    get_registry()
    error_report()
    conn.close(upload_batch_guid)