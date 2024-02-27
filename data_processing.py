from datetime import datetime
from config import oracle_ip, oracle_database, oracle_username, oracle_password, full_upload
from utils import error_handler, is_case_duplicate, Database 
from service_methods import get_reports, get_messages
import cx_Oracle
import xml.etree.ElementTree as Et
from data_utils import get_value, find_node_and_get_value, find_node_and_accumulate, remove_extra_symbols

name_history = []
legal_cases = []


def process_annulled_reports(reports, annulled_reports, main_id_title, tablename, upload_batch_id):
    reports_to_process = reports
    for annulled_report in annulled_reports:
        error_handler('I', 900,
                      f"Получено сообщение {annulled_report['AnnulmentId']} об аннулировании {annulled_report['WhatToAnnul']} для таблицы {tablename}", None,
                      None, None, upload_batch_id)
        for report in reports_to_process:
            if report[main_id_title] == annulled_report['WhatToAnnul']:
                report['IsAnnulled'] = annulled_report['AnnulmentId']
                report['DateAnnulled'] = annulled_report['AnnulmentDate']
                report['AnnulmentReason'] = annulled_report['AnnulmentReason']

    if full_upload is False:  # if we want to receive new data we have to look for original messages in the db and mark them as annulled
        #conn = None
        try:
            conn = Database() #connect_to_database(oracle_ip, oracle_database, oracle_username, oracle_password)
            cur = conn.connection.cursor()
            all_annuled = len(annulled_reports)
            cur_annul = 0
            for annulled_report in annulled_reports:
                error_handler('I', 111, f"Поиск аннулируемого сообщения {annulled_report['WhatToAnnul']} в {tablename}: {cur_annul}/{all_annuled}", None, None, None, upload_batch_id)
                cur_annul+=1
                cur.execute(f"select 1 from {tablename} where {main_id_title} = :myinput", myinput=annulled_report['WhatToAnnul'])
                row = cur.fetchone()
                if row and row[0] == 1:
                    try:
                        sql = f'update {tablename} set isannulled = :myinput1, DateAnnulled = :myinput2, AnnulmentReason = :myinput3 where {main_id_title} = :myinput4'
                        cur.execute(sql, [annulled_report['AnnulmentId'], annulled_report['AnnulmentDate'], annulled_report['AnnulmentReason'], annulled_report['WhatToAnnul']])
                        conn.connection.commit()
                        #print(f"\U0001F535 201: Report {annulled_report['WhatToAnnul']} has been annulled. Table {tablename}")
                        error_handler('I', 201, f"Аннулировано сообщение / отчёт {annulled_report['WhatToAnnul']} для таблицы {tablename}", None, None, None, upload_batch_id)
                    except cx_Oracle.Error as error:
                        #print(f"\U0001F534 910: Error while trying to annul message {annulled_report['WhatToAnnul']} in the DB: ", str(error))
                        error_handler('E', 910, f"Ошибка #3 при попытке пометить сообщение / отчёт {annulled_report['WhatToAnnul']} как аннулированный: " + str(error), None, None, None, upload_batch_id)
                else:
                    if tablename == 'gis_efrsb_messages':
                        try:
                            cur.execute(f"select 1 from gis_efrsb_extr_bnkr where {main_id_title} = :myinput", myinput=annulled_report['WhatToAnnul'])
                            row = cur.fetchone()
                            if row is None:
                                #print(f"\U0001F534 909: Couldn't find message {annulled_report['WhatToAnnul']} to annul in the DB for table {tablename}: ")
                                error_handler('E', 909, f"Не удалось найти сообщение / отчёт для аннулирования  {annulled_report['WhatToAnnul']} для таблицы {tablename}", None, None, None, upload_batch_id)
                        except cx_Oracle.Error as error:
                            #print(f"\U0001F534 910: Error #2 while trying to annul message {annulled_report['WhatToAnnul']} in the DB: ", str(error))
                            error_handler('E', 910, f"Ошибка #2 при попытке пометить сообщение / отчёт {annulled_report['WhatToAnnul']} как аннулированный: " + str(error), None, None, None, upload_batch_id)
                    elif tablename == 'gis_efrsb_extr_bnkr':
                        try:
                            cur.execute(f"select 1 from gis_efrsb_messages where {main_id_title} = :myinput", myinput=annulled_report['WhatToAnnul'])
                            row = cur.fetchone()
                            if row is None:
                                #print(f"\U0001F534 909: Couldn't find message {annulled_report['WhatToAnnul']} to annul in the DB for table {tablename}: ")
                                error_handler('E', 909, f"Не удалось найти сообщение / отчёт для аннулирования  {annulled_report['WhatToAnnul']} для таблицы {tablename}", None, None, None, upload_batch_id)
                        except cx_Oracle.Error as error:
                            #print(f"\U0001F534 910: Error #2 while trying to annul message {annulled_report['WhatToAnnul']} in the DB: ", str(error))
                            error_handler('E', 910, f"Ошибка #2 при попытке пометить сообщение / отчёт {annulled_report['WhatToAnnul']} как аннулированный: " + str(error), None, None, None, upload_batch_id)
                    else:
                        #print(f"\U0001F534 909: Couldn't find message {annulled_report['WhatToAnnul']} to annul in the DB: ")
                        error_handler('E', 909, f"Не удалось найти сообщение / отчёт для аннулирования  {annulled_report['WhatToAnnul']} для таблицы {tablename}", None, None, None, upload_batch_id)

        except cx_Oracle.Error as error:
            #print(f"\U0001F534 910: Error has occurred while trying to mark a message as annulled in the DB: ", str(error))
            error_handler('E', 910, 'Ошибка #1 при попытке пометить сообщение / отчёт как аннулированный: ' + str(error), None, None, None, upload_batch_id)
        # finally:
        #     if conn:
        #         conn.close()
    return reports_to_process


def get_message_payload(title, node):
    value = ''
    if title == 'ReceivingCreditorDemand':
        demand_sum = None
        bank = None
        for my_type in node:
            for child in my_type:
                if child.tag == 'DemandSum':
                    demand_sum = get_value(child.text, 'roubles')
                if child.tag == 'CreditorName':
                    bank = get_value(child.text.title(), 'string')

        return f'{bank or "Банк"} выставил требование в размере {demand_sum}'

    if title == 'DeliberateBankruptcy':
        deliberate_bankruptcy = None
        fake_bankruptcy = None
        for my_type in node:
            for child in my_type:
                if child.tag == 'DeliberateBankruptcySigns':
                    if child.text == 'NotSearched':
                        deliberate_bankruptcy = 'Проверка на предмет преднамеренного банкротства не проводилась'
                    elif child.text == 'Found':
                        deliberate_bankruptcy = 'Выявлены признаки преднамеренного банкротства'
                    elif child.text == 'NotFound':
                        deliberate_bankruptcy = 'Не выявлены признаки преднамеренного банкротства'
                    else:
                        deliberate_bankruptcy = child.text
                if child.tag == 'FakeBankruptcySigns':
                    if child.text == 'NotSearched':
                        fake_bankruptcy = 'Проверка на предмет фиктивного банкротства не проводилась'
                    elif child.text == 'Found':
                        fake_bankruptcy = 'Выявлены признаки фиктивного банкротства'
                    elif child.text == 'NotFound':
                        fake_bankruptcy = 'Не выявлены признаки фиктивного банкротства'
                    else:
                        fake_bankruptcy = child.text
                if child.tag == 'Text':
                    deliberate_bankruptcy = child.text

        return_string = None
        if deliberate_bankruptcy == None and fake_bankruptcy == None:
            pass
        elif deliberate_bankruptcy == None and fake_bankruptcy != None:
            return_string = remove_extra_symbols(fake_bankruptcy)
        elif deliberate_bankruptcy != None and fake_bankruptcy == None:
            return_string = remove_extra_symbols(deliberate_bankruptcy)
        else:
            return_string = remove_extra_symbols(deliberate_bankruptcy + '. ' + fake_bankruptcy)

        return return_string

    if title == 'BankOpenAccountDebtor':
        bank = None
        for my_type in node:
            for child in my_type:
                if child.tag == 'Name':
                    bank = child.text.title()
        return 'В "' + bank + '" открыт специальный счёт должника'

    if title == 'Annul':
        message_to_annul = None
        reason = None
        for my_type in node:
            for child in my_type:
                if child.tag == 'Text':
                    reason = remove_extra_symbols(child.text)
                if child.tag == 'IdAnnuledMessage':
                    message_to_annul = int(child.text)
        return [message_to_annul, reason]

    if title == 'Other':
        for my_type in node:
            text = None
            for child in my_type:
                if child.tag == 'Text':
                    text = remove_extra_symbols(child.text)
            return text
    else:
        return remove_extra_symbols(value)


def extrajudicial_bankruptcy(node, message_type, id_message, id_debtor, upload_batch_id):
    monetary_obligations, obligatory_payments, banks, message_reference, payload = (None,) * 5

    if message_type == 'StartOfExtrajudicialBankruptcy':
        monetary_obligations = 0.00
        obligatory_payments = 0.00
        # all obligations from this person regardless whether they are an entrepreneur or a natural person
        monetary_obligations = find_node_and_accumulate(node, 'MonetaryObligation', 'TotalSum', 'float')
        # all obligations towards the government such as taxes
        obligatory_payments = find_node_and_accumulate(node, 'ObligatoryPayment', 'Sum', 'float')
        banks = find_node_and_accumulate(node, 'Bank', 'Name', 'string')
    elif message_type == 'TerminationOfExtrajudicialBankruptcy':
        message_reference = find_node_and_get_value(node, 'TerminationOfExtrajudicialBankruptcy', 'StartOfExtrajudicialBankruptcyMessageNumber')
        if message_reference is None:
            error_handler('E', 302, 'Не найден тег StartOfExtrajudicialBankruptcyMessageNumber', id_debtor, id_message, message_type, upload_batch_id)
        elif message_reference == 'Not found':
            error_handler('E', 301, 'Не найден тег TerminationOfExtrajudicialBankruptcy', id_debtor, id_message, message_type, upload_batch_id)
        else:
            message_reference = int(message_reference)
    elif message_type == 'CompletionOfExtrajudicialBankruptcy':
        message_reference = find_node_and_get_value(node, 'CompletionOfExtrajudicialBankruptcy', 'StartOfExtrajudicialBankruptcyMessageNumber')
        if message_reference is None:
            error_handler('E', 302, 'Не найден тег StartOfExtrajudicialBankruptcyMessageNumber', id_debtor, id_message,
                          message_type, upload_batch_id)
        elif message_reference == 'Not found':
            error_handler('E', 303, 'Не найден тег CompletionOfExtrajudicialBankruptcy', id_debtor, id_message,
                          message_type, upload_batch_id)
        else:
            message_reference = int(message_reference)
        payload = find_node_and_get_value(node, 'CompletionOfExtrajudicialBankruptcy', 'Text')
    elif message_type == 'ReturnOfApplicationOnExtrajudicialBankruptcy2':
        payload = find_node_and_accumulate(node, 'ReturnReason', 'Description', 'string')
    else:
        if message_type != 'ReturnOfApplicationOnExtrajudicialBankruptcy':
            error_handler('E', 304, 'Неизвестный тип во внесудебном банкротстве', id_debtor, id_message,
                          message_type, upload_batch_id)
    return monetary_obligations, obligatory_payments, banks, message_reference, payload


def get_debtor_messages(client, bankruptid, upload_batch_id, date):
    response = get_messages(client, bankruptid, date, upload_batch_id)
    messages = []
    messages_extr = []
    messages_annulled = []
    extrajudicial_message_types = ['ReturnOfApplicationOnExtrajudicialBankruptcy','ReturnOfApplicationOnExtrajudicialBankruptcy2', 'StartOfExtrajudicialBankruptcy', 'TerminationOfExtrajudicialBankruptcy', 'CompletionOfExtrajudicialBankruptcy']

    if response:
        error_handler('I', 205, 'Получены сообщения', bankruptid, None, None, upload_batch_id)
        root = Et.fromstring(response)
        for report in root:
            id_message, message_guid, legal_case_number, message_type, publish_date, court_id_decree,\
                monetary_obligations, obligatory_payments, banks, message_reference, court_name, court_decision_date, \
                payload = (None,) * 13

            for child in report:
                if child.tag == 'Id':
                    id_message = int(child.text)
                if child.tag == 'MessageGUID':
                    message_guid = child.text
                if child.tag == 'CaseNumber':
                    legal_case_number = get_value(child.text, 'string')
                if child.tag == 'PublishDate':
                    publish_date = datetime.strptime(child.text[:10], '%Y-%m-%d')
                if child.tag == 'MessageInfo':
                    message_type = child.attrib["MessageType"]
                    if message_type == 'ArbitralDecree':
                        for my_type in child:
                            for decree_child in my_type:
                                if decree_child.tag == 'DecisionType':
                                    court_id_decree = int(decree_child.attrib["Id"])
                                    if court_id_decree == 31:
                                        error_handler('E', 203, '31 тип судебного решения об отмене прошлых решений', bankruptid, id_message, message_type, upload_batch_id)
                                if decree_child.tag == 'CourtDecree':
                                    for decree_tag in decree_child:
                                        if decree_tag.tag == 'CourtName':
                                            court_name = decree_tag.text.strip()
                                        if decree_tag.tag == 'DecisionDate':
                                            court_decision_date = datetime.strptime(decree_tag.text, '%Y-%m-%d')
                                if decree_child.tag == 'Text':
                                    payload = remove_extra_symbols(decree_child.text)
                                if decree_child.tag == 'CancelledMessages': #для 31 типа об отмене судебных решений
                                    for cancelled_message in decree_child:
                                        print("AnnulmentId", id_message)
                                        print("WhatToAnnul", cancelled_message.text)
                                        print(messages)
                                        annulment_info = {"AnnulmentId": id_message,
                                                          "WhatToAnnul": int(cancelled_message.text),
                                                          "AnnulmentDate": court_decision_date,
                                                          "AnnulmentReason": payload}
                                        messages_annulled.append(annulment_info)
                                if decree_child.tag == 'CitizenNotReleasedFromResponsibility' and court_id_decree == 31:
                                    if decree_child.text: #когда-то значение пишут внутри тега
                                        payload = decree_child.text
                                    else: #а иногда значение пишут в атрибуте
                                        resp_value = decree_child.attrib["{http://www.w3.org/2001/XMLSchema-instance}nil"]
                                        if resp_value == 'true':
                                            payload = 'Не применять в отношении гражданина правило об освобождении от исполнения обязательств'
                                        if resp_value == 'false':
                                            payload = 'Применить правило об освобождении от исполнения обязательств'
                    elif message_type in extrajudicial_message_types:
                        monetary_obligations, obligatory_payments, banks, message_reference, payload = extrajudicial_bankruptcy(child, message_type, bankruptid, id_message, upload_batch_id)
                    else:
                        payload = get_message_payload(message_type, child)
            if message_type:
                if message_type != 'Annul':  # annulment messages are not to be uploaded to the database
                    if message_type in extrajudicial_message_types:
                        extr_message = {"IdMessage": id_message,
                                        "BankruptId": bankruptid,
                                        "MessageType": message_type,
                                        "PublishDate": publish_date,
                                        "MonetaryObligations": monetary_obligations,
                                        "ObligatoryPayments": obligatory_payments,
                                        "Banks": banks,
                                        "MessageReference": message_reference,
                                        "Payload": payload,
                                        "MessageGuid": message_guid,
                                        "IsAnnulled": None,
                                        "DateAnnulled": None,
                                        "AnnulmentReason": None,
                                        "uploadBatchId": upload_batch_id}
                        messages_extr.append(extr_message)
                    else:
                        message_info = {"IdMessage": id_message,
                                        "BankruptId": bankruptid,
                                        "LegalCaseNumber": legal_case_number,
                                        "MessageType": message_type,
                                        "PublishDate": publish_date,
                                        "CourtIdDecree": court_id_decree,
                                        "CourtName": court_name,
                                        "CourtDecisionDate": court_decision_date,
                                        "Payload": payload,
                                        "MessageGuid": message_guid,
                                        "IsAnnulled": None,
                                        "DateAnnulled": None,
                                        "AnnulmentReason": None,
                                        "uploadBatchId": upload_batch_id}
                        messages.append(message_info)
                else:
                    annulment_info = {"AnnulmentId": id_message,
                                      "WhatToAnnul": payload[0],
                                      "AnnulmentDate": publish_date,
                                      "AnnulmentReason": payload[1]}
                    messages_annulled.append(annulment_info)

    messages = process_annulled_reports(messages, messages_annulled, 'IdMessage', 'gis_efrsb_messages', upload_batch_id)
    messages_extr = process_annulled_reports(messages_extr, messages_annulled, 'IdMessage', 'gis_efrsb_extr_bnkr', upload_batch_id)

    return messages, messages_extr


def get_debtor_reports(client, bankruptid, upload_batch_id, date):
    response = get_reports(client, bankruptid, date, upload_batch_id)
    reports = []
    reports_annulled = []

    if response:
        error_handler('I', 405, 'Получены отчёты', bankruptid, None, None, upload_batch_id)
        root = Et.fromstring(response)
        for report in root:
            report_id = ''  # extracting the message id regardless of the type
            duplicate_report = False
            for child in report:
                if child.tag == 'ReportId':
                    report_id = int(child.text)
                    break
                    
            if duplicate_report is False:
                if report.tag in ['FinalAuReport', 'FinalReport']:
                    report_guid, procedure_type, report_type, date_create, id_legal_case, legal_case_number, decision, \
                        court_name, accrued_total, is_legal_case_closed, legal_case_state = (None,) * 11
                    for child in report:
                        if child.tag == 'Guid':
                            report_guid = child.text
                        if child.tag == 'ProcedureType':
                            procedure_type = child.text
                        if child.tag == 'ReportType':
                            report_type = child.text
                        if child.tag == 'DateCreate':
                            date_create = datetime.strptime(child.text, '%Y-%m-%d')
                        if child.tag == 'IdLegalCase':
                            id_legal_case = int(child.text)
                        if child.tag == 'LegalCaseNumber':
                            legal_case_number = get_value(child.text, 'string')
                        if child.tag == 'CourtName':
                            court_name = child.text.strip()
                        if child.tag == 'Body':
                            for bodychildren in child:
                                if bodychildren.tag == 'Decision':
                                    decision = bodychildren.text
                                if bodychildren.tag == 'RegistryRequirements':
                                    for registry in bodychildren:
                                        if registry.tag == 'AccruedTotal':
                                            accrued_total = round(float(registry.text), 2)
                                if bodychildren.tag == 'IsLegalCaseClosed':
                                    is_legal_case_closed = 1 if bodychildren.text == 'true' else None
                                if bodychildren.tag == 'LegalCaseState':
                                    legal_case_state = bodychildren.text
                    report_info = {"ReportId": report_id,
                                   "BankruptId": bankruptid,
                                   "ReportType": report_type,
                                   "ProcedureType": procedure_type,
                                   "Decision": decision,
                                   "DateCreate": date_create,
                                   "AccruedTotal": accrued_total,
                                   "IdLegalCase": id_legal_case,
                                   "LegalCaseNumber": legal_case_number,
                                   "CourtName": court_name,
                                   "IsLegalCaseClosed": is_legal_case_closed,
                                   "LegalCaseState": legal_case_state,
                                   "ReportGuid": report_guid,
                                   "IsAnnulled": None,
                                   "DateAnnulled": None,
                                   "AnnulmentReason": None,
                                   "uploadBatchId": upload_batch_id}
                    reports.append(report_info)

                elif report.tag in ['AnnulmentAuReport', 'AnnulmentReport']:
                    annulment_report_id, report_to_annul, annulment_date, annulment_reason = (None,) * 4
                    for child in report:
                        if child.tag == 'ReportId':
                            annulment_report_id = int(child.text)
                        if child.tag == 'DateCreate':
                            annulment_date = datetime.strptime(child.text, '%Y-%m-%d')
                        if child.tag == 'Body':
                            for bodychildren in child:
                                if bodychildren.tag == 'ReportForAnnulmentId':
                                    report_to_annul = int(bodychildren.text)
                                if bodychildren.tag == 'Text':
                                    annulment_reason = remove_extra_symbols(bodychildren.text)
                    annulment_info = {"AnnulmentId": annulment_report_id,
                                      "WhatToAnnul": report_to_annul,
                                      "AnnulmentDate": annulment_date,
                                      "AnnulmentReason": annulment_reason}
                    reports_annulled.append(annulment_info)
                else:
                    error_handler('E', 401, f"Получен необрабатываемый тип отчета: {report.tag}", bankruptid, report_id,
                                  report.tag, upload_batch_id)
    else:
        error_handler('I', 404, f"Не найдены отчеты за указанный период", bankruptid, None, None, upload_batch_id)

    if reports_annulled:
        reports = process_annulled_reports(reports, reports_annulled, 'ReportId', 'gis_efrsb_reports', upload_batch_id)
        return reports

    return reports


def populate_cases(case, bankrupt_id, upload_batch_guid):
    for legal_case in case:
        lc_number = 'б/н'
        if legal_case['Number']:
            lc_number = legal_case['Number'].strip()
        lc_date = get_value(legal_case['DateCreate'], 'datestamp')
        lc_court = get_value(legal_case['Court'].strip(), 'string')
        case = {'BankruptId': bankrupt_id, 'CaseNumber': lc_number, 'CreatedDate': lc_date, 'Court': lc_court, 'uploadBatchId': upload_batch_guid}

    
        duplicate_case = is_case_duplicate(legal_cases, case, bankrupt_id, upload_batch_guid)
        if duplicate_case is False:
            legal_cases.append(case)


def populate_name_history(bankrupt_id, fullname, upload_batch_guid):
    name = {'BankruptId': bankrupt_id, 'fullname': fullname}
    
    if name not in name_history:
        name_history.append(name)


def debtor_parser(data, my_type, upload_batch_guid):
    guid, bankrupt_id, inn, ogrn, snils, category, region, address, fullname, lastname, firstname, patronymicname, birthdate, birthplace = (None,) * 14
    guid = data['Guid']
    bankrupt_id = data['BankruptId']

    inn = get_value(data['INN'], 'string')
    category = get_value(data['CategoryCode'], 'string')
    region = get_value(data['Region'], 'string')
    if my_type == 'DebtorCompany':
        ogrn = get_value(data['OGRN'], 'string')
        address = data['LegalAddress']
        fullname = get_value(data['FullName'], 'string')
    else:
        snils = get_value(data['SNILS'], 'string')
        ogrn = get_value(data['OGRNIP'], 'string')
        address = get_value(data['Address'], 'string')
        lastname = get_value(data['LastName'], 'upper string')
        firstname = get_value(data['FirstName'], 'upper string')
        patronymicname = get_value(data['MiddleName'], 'upper string')
        fullname = f"{lastname or ''} {firstname or ''} {patronymicname or ''}".strip()
        populate_name_history(bankrupt_id, fullname, upload_batch_guid)
        birthdate = get_value(data['Birthdate'], 'date')
        birthplace = get_value(data['Birthplace'], 'string')
        if data['NameHistory'] is not None:
            for name in data['NameHistory']['NameHistoryItem']:
                populate_name_history(bankrupt_id, name.upper(), upload_batch_guid)

    if data['LegalCaseList'] is not None:
        populate_cases(data['LegalCaseList']['LegalCaseInfo'], bankrupt_id, upload_batch_guid)

    person = {'BankruptId': bankrupt_id,
              'category': category,
              'lastname': lastname,
              'firstname': firstname,
              'patronymicname': patronymicname,
              'fullname': fullname,
              'inn': inn,
              'snils': snils,
              'ogrn': ogrn,
              'region': region,
              'address': address,
              'birthdate': birthdate,
              'birthplace': birthplace,
              'guid': guid,
              'uploadBatchId': upload_batch_guid}
    return person, bankrupt_id
