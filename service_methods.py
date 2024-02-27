import time
from utils import error_handler
from datetime import datetime
from config import full_upload


def get_messages(client, bankruptid, date, upload_batch_guid):
    for i in range(6):
        try:
            
            error_handler('I', 913,
                            f"Отправка запроса получения сообщений",
                            bankruptid, None,
                            None, upload_batch_guid)
            response = client.service.GetDebtorMessagesContentForPeriodByIdBankrupt(bankruptid)
            return response
        except Exception as e:
            error_handler('E', 914,
                          f'Ошибка обработки сервером запроса на получение сообщений {str(e)}. Попытка № {str(i + 2)}', bankruptid, None, None, upload_batch_guid)

            if i == 5:
                error_handler('E', 402, 'Ошибка подключения к серверу при отправке запроса GetDebtorMessagesContentForPeriodByIdBankrupt. Превышено количество попыток: ' + str(e), bankruptid, None, None, upload_batch_guid)
                raise  # give up after 4 attempts
            time.sleep(2)


def get_reports(client, bankruptid, date, upload_batch_guid):
    for i in range(6):
        try:
            error_handler('I', 913,
                            f"Отправка запроса получения отчётов",
                            bankruptid, None,
                            None, upload_batch_guid)
            response = client.service.GetDebtorReportsContentForPeriodByIdBankrupt(bankruptid)
            return response
        except Exception as e:
            error_handler('E', 914,
                          f'Ошибка обработки сервером запроса на получение отчётов {str(e)}. Попытка № {str(i + 2)}',
                          bankruptid, None, None, upload_batch_guid)
            if i == 5:
                error_handler('E', 402, 'Ошибка подключения к серверу при отправке запроса GetDebtorReportsContentForPeriodByIdBankrupt. Превышено количество попыток: ' + str(e), bankruptid, None, None, upload_batch_guid)
                raise  # give up after 4 attempts
            time.sleep(2)


def get_registry_by_period(client, start: datetime, end: datetime, upload_batch_guid):
    start_string = start.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_string = end.strftime('%Y-%m-%dT%H:%M:%SZ')
    for i in range(6):
        try:
            error_handler('I', 913,
                          f"Отправка запроса получения банкротов с {start_string} по {end_string}",
                          None, None,
                          None, upload_batch_guid)
            response = client.service.GetDebtorsByLastPublicationPeriod(start_string, end_string)
            return response
        except Exception as e:
            error_handler('E', 914,
                          f'Ошибка обработки сервером запроса на получение банкротов {str(e)}. Попытка № {str(i + 2)}',
                          None, None, None, upload_batch_guid)
            if i == 5:
                error_handler('E', 106, 'Ошибка подключения к серверу при отправке запроса GetDebtorsByLastPublicationPeriod. Превышено количество попыток: ' + str(e), None, None, None, upload_batch_guid)
                raise  # give up after 4 attempts
            time.sleep(2)


def connect_to_efrsb(session, client, web_service_url, transport, upload_batch_guid, settings):
    for i in range(6):
        try:
            error_handler('I', 913,
                          f"Подключение к серверу",
                          None, None,
                          None, upload_batch_guid)
            my_client = client(web_service_url, transport=transport(session=session), settings=settings)
            return my_client
        except Exception as e:
            error_handler('E', 914,
                          f'Ошибка обработки сервером запроса на подключение к серверу {str(e)}. Попытка № {str(i + 2)}',
                          None, None, None, upload_batch_guid)
            if i == 5:
                error_handler('E', 911, f'Ошибка подключения к серверу. Превышено количество попыток: {str(e)}', None, None, None, upload_batch_guid)
                raise  # give up after 4 attempts
            time.sleep(2)
