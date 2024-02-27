CREATE OR REPLACE PACKAGE gis_efrsb AS

  TYPE list_of_debtors IS TABLE OF NUMBER INDEX BY PLS_INTEGER;

  TYPE t_cus_record IS RECORD
    (
     icusnum        cus.icusnum%TYPE,
     ccusflag       cus.ccusflag%TYPE,
     ccusname       cus.ccusname%TYPE,
     DCUSBIRTHDAY   cus.dcusbirthday%TYPE,
     ccusnumnal     cus.ccusnumnal%TYPE,
     ccussnils      cus.ccussnils%TYPE,
     ccusksiva      cus.ccusksiva%TYPE
    );

  TYPE t_cus_record_col IS TABLE OF t_cus_record INDEX BY PLS_INTEGER;
  TYPE list_of_cases IS TABLE OF VARCHAR2(300);

  debugging BOOLEAN := FALSE;

  PROCEDURE p_set_debugging(p_debugging BOOLEAN);

  PROCEDURE p_log_efrsb(p_time    DATE,
                      p_type    VARCHAR2,
                      p_funcion VARCHAR2,
                      p_client  NUMBER,
                      p_debtor  NUMBER,
                      p_mes     VARCHAR2,
                      p_user    VARCHAR2);

  TYPE t_found_cus_debtor IS RECORD
  (
       iccusnum NUMBER,
       debtorid NUMBER,
       status NUMBER
  );

  TYPE t_found_cus_debtor_tab IS TABLE OF t_found_cus_debtor INDEX BY PLS_INTEGER;

  FUNCTION f_has_active_cases(p_debtorid NUMBER, p_cus_type NUMBER) RETURN VARCHAR2;

  FUNCTION f_get_debtor_id(p_type NUMBER,
                           p_inn VARCHAR2,
                           p_snils VARCHAR2,
                           p_ogrn VARCHAR2,
                           p_birthdate DATE,
                           p_fullname VARCHAR2) RETURN list_of_debtors;

  FUNCTION f_is_debtor(p_type      NUMBER,
                     p_inn       VARCHAR2,
                     p_snils     VARCHAR2,
                     p_ogrn      VARCHAR2,
                     p_birthdate DATE,
                     p_fullname  VARCHAR2) RETURN NUMBER;

  FUNCTION f_find_debtors RETURN VARCHAR2;
  
  FUNCTION f_get_leg_cases(p_debtor_id NUMBER) RETURN list_of_cases PIPELINED;

END gis_efrsb;
/
CREATE OR REPLACE PACKAGE BODY gis_efrsb is

  /*Вывод всех сообщений dnms_output в пакете для дебага*/
  PROCEDURE p_set_debugging(p_debugging BOOLEAN) IS
  BEGIN
    debugging := p_debugging;
  END p_set_debugging;

  /*Логирование событий*/
  PROCEDURE p_log_efrsb(p_time    DATE,
                        p_type    VARCHAR2,
                        p_funcion VARCHAR2,
                        p_client  NUMBER,
                        p_debtor  NUMBER,
                        p_mes     VARCHAR2,
                        p_user    VARCHAR2) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
  
  BEGIN
    INSERT INTO xxi.gis_efrsb_log
      (dtime, ctype, cfuncion, icusnum, ibankruptid, cmessage, cuser)
    values
      (p_time, p_type, p_funcion, p_client, p_debtor, p_mes, p_user);
    COMMIT;
  END p_log_efrsb;

  /*Поиск открытого судебного / внесудебного дела*/
  PROCEDURE p_get_legal_cases(p_bankrupt_id NUMBER,
                              p_result      OUT NUMBER /*1 - есть незакрытое дело, 0 - производства по делам завершены*/) IS
    /*одновременно по одному банкроту не может быть более одного дела, поэтому ищем все сообщения и отчёты по должнику.
    если последнее сообщение или отчёт о закрытии, то возвращаем 0.
    если сообщения о возобновлении или другие типы, то возвращаем 1 */
  
    CURSOR get_extr_documents(debtor_id NUMBER) IS
      SELECT messagetype
        FROM gis_efrsb_extr_bnkr
       WHERE bankruptid = debtor_id
         AND ISANNULLED IS NULL
       ORDER BY publishdate DESC
       FETCH NEXT 1 ROWS ONLY;
  
    CURSOR get_documents(debtor_id NUMBER) IS
      SELECT LEGALCASENUMBER,
             MAX(case
                   when "status" is null then
                    "docdate"
                   else
                    null
                 end) "messagedate",
             MAX(case
                   when "status" = 'Closed' then
                    "docdate"
                   else
                    null
                 end) "closeddate",
             MAX(case
                   when "status" = 'Resumed' then
                    "docdate"
                   else
                    null
                 end) "Resumeddate"
        FROM (SELECT LEGALCASENUMBER,
                     datecreate as "docdate",
                     CASE
                       WHEN ISLEGALCASECLOSED = 1 THEN
                        'Closed'
                       ELSE
                        'Resumed'
                     END "status"
                FROM gis_efrsb_reports
               WHERE bankruptid = debtor_id
                 AND isannulled IS NULL
              UNION ALL
              SELECT LEGALCASENUMBER,
                     nvl(COURTDECISIONDATE, PUBLISHDATE) as "docdate",
                     CASE
                       WHEN COURTIDDECREE in (8, 17, 21, 25) THEN
                        'Closed'
                       WHEN COURTIDDECREE = 3 THEN
                        'Resumed'
                       ELSE
                        NULL
                     END "status"
                FROM gis_efrsb_messages
               WHERE bankruptid = debtor_id
                 AND isannulled IS NULL)
       group by LEGALCASENUMBER;
  
  BEGIN
    IF debugging THEN
      DBMS_OUTPUT.PUT_LINE(chr(10) ||
                           'Клиент найден в реестре банкротов. Проверяем судебные и внесудебные дела.');
    END IF;
    IF debugging THEN
      DBMS_OUTPUT.PUT_LINE('Банкрот ' || to_char(p_bankrupt_id));
    END IF;
  
    FOR doc IN get_documents(p_bankrupt_id) LOOP
      IF doc."messagedate" IS NOT NULL AND doc."closeddate" IS NULL THEN
        IF debugging THEN
          DBMS_OUTPUT.PUT_LINE('Дело ' || doc.LEGALCASENUMBER ||
                               ' актуально');
        END IF;
        p_result := 1;
      END IF;
      IF doc."Resumeddate" >= doc."closeddate" THEN
        IF debugging THEN
          DBMS_OUTPUT.PUT_LINE('Дело ' || doc.LEGALCASENUMBER ||
                               ' актуально');
        END IF;
        p_result := 1;
      END IF;
    END LOOP;
  
    FOR extr IN get_extr_documents(p_bankrupt_id) LOOP
      IF debugging THEN
        DBMS_OUTPUT.PUT_LINE('Поиск внесудебных сообщений. Последнее с типом: ' ||
                             extr.messagetype);
      END IF;
      IF extr.messagetype = 'StartOfExtrajudicialBankruptcy' THEN
        p_result := 1;
      END IF;
    END LOOP;
  
    IF debugging AND p_result = 1 THEN
      DBMS_OUTPUT.PUT_LINE('Есть текущее дело');
    END IF;
  
  EXCEPTION
    WHEN OTHERS THEN
      p_log_efrsb(SYSDATE,
                  'E',
                  'p_get_legal_cases',
                  null,
                  null,
                  to_char(SQLCODE) || ' ' || to_char(SQLERRM),
                  USER);
      p_result := -1;
  END p_get_legal_cases;

  /*Возвращает строковый результат поиска активных дел. Используется для отчёта 1%52*/
  FUNCTION f_has_active_cases(p_debtorid NUMBER, p_cus_type NUMBER)
    RETURN VARCHAR2 IS
    v_result NUMBER;
    v_return VARCHAR2(50) := 'Ошибка определения статуса банкротства';
  BEGIN
    IF p_cus_type = 2 THEN
      RETURN 'Компания-банкрот';
    END IF;
  
    p_get_legal_cases(p_debtorid, v_result);
    IF v_result = 1 THEN
      v_return := 'Идёт производство';
    ELSE
      v_return := 'Производства завершены';
    END IF;
  
    RETURN v_return;
  END f_has_active_cases;

  /*Функция возвращает коллекцию банкротов, найденных по полученным параметрам. Используется коллекция, т.к. есть редкие случаи дубликатов в реестре банкротов*/
  FUNCTION f_get_debtor_id(p_type      NUMBER /*1-ФЛ, 2-ЮЛ, 4-ИП*/,
                           p_inn       VARCHAR2,
                           p_snils     VARCHAR2,
                           p_ogrn      VARCHAR2,
                           p_birthdate DATE,
                           p_fullname  VARCHAR2) RETURN list_of_debtors IS
    found_debtors list_of_debtors;
    CURSOR c_inn_snils_search(p_inn VARCHAR2, p_snils VARCHAR2) IS
      SELECT bankruptid
        FROM gis_efrsb_debtors
       WHERE (p_inn IS NOT NULL AND inn = p_inn)
          OR (p_snils IS NOT NULL AND snils = p_snils);
    CURSOR c_name_birthd_search(p_fullname VARCHAR2, p_birthdate DATE) IS
      SELECT d.bankruptid
        FROM gis_efrsb_debtors d
       RIGHT JOIN gis_efrsb_names n
          on d.bankruptid = n.bankruptid
       WHERE translate(n.fullname, 'ЙЁ', 'ИЕ') =
             translate(upper(p_fullname), 'ЙЁ', 'ИЕ')
         and d.BIRTHDATE = p_birthdate;
    CURSOR c_inn_snils_ogrn_search(p_inn VARCHAR2, p_ogrn VARCHAR2, p_snils VARCHAR2) IS
      SELECT bankruptid
        FROM gis_efrsb_debtors
       WHERE (p_inn IS NOT NULL AND inn = p_inn)
          OR (p_ogrn IS NOT NULL AND ogrn = p_ogrn)
          OR (p_snils IS NOT NULL AND snils = p_snils);
    CURSOR c_inn_ogrn_search(p_inn VARCHAR2, p_ogrn VARCHAR2) IS
      SELECT bankruptid
        FROM gis_efrsb_debtors
       WHERE (p_inn IS NOT NULL AND inn = p_inn)
          OR (p_ogrn IS NOT NULL AND ogrn = p_ogrn);
  
  BEGIN
    IF p_type = 1 THEN
      IF p_inn IS NOT NULL OR p_snils IS NOT NULL THEN
        IF debugging THEN
          DBMS_OUTPUT.PUT_LINE('ФЛ. Поиск по ИНН и СНИЛС.');
        END IF;
        FOR debtor IN c_inn_snils_search(p_inn,
                                         translate(p_snils, 'X- ', 'X')) LOOP
          found_debtors(found_debtors.COUNT + 1) := debtor.bankruptid;
        END LOOP;
      ELSIF p_fullname IS NOT NULL AND p_birthdate IS NOT NULL THEN
        IF debugging THEN
          DBMS_OUTPUT.PUT_LINE('ФЛ. Поиск по ФИО и дате рождения.');
        END IF;
        FOR debtor IN c_name_birthd_search(p_fullname, p_birthdate) LOOP
          found_debtors(found_debtors.COUNT + 1) := debtor.bankruptid;
        END LOOP;
      ELSE
        IF debugging THEN
          DBMS_OUTPUT.PUT_LINE('ФЛ. Нет параметров для поиска.');
        END IF;
        found_debtors(-1) := null;
      END IF;
    
    ELSIF p_type = 4 THEN
      IF debugging THEN
        DBMS_OUTPUT.PUT_LINE('ИП. Ищем по ИНН, ОГРН, СНИЛС.');
      END IF;
      FOR debtor IN c_inn_snils_ogrn_search(p_inn, p_ogrn, p_snils) LOOP
        found_debtors(found_debtors.COUNT + 1) := debtor.bankruptid;
      END LOOP;
    ELSE
      IF debugging THEN
        DBMS_OUTPUT.PUT_LINE('ЮЛ. Ищем по ИНН, ОГРН.');
      END IF;
      FOR debtor IN c_inn_ogrn_search(p_inn, p_ogrn) LOOP
        found_debtors(found_debtors.COUNT + 1) := debtor.bankruptid;
      END LOOP;  
    END IF;
  
    RETURN found_debtors;
  
  EXCEPTION
    WHEN OTHERS THEN
      p_log_efrsb(SYSDATE,
                  'E',
                  'F_GET_DEBTOR_ID',
                  null,
                  null,
                  'Клиент ' || p_fullname || ' ' ||
                  to_char(p_birthdate, 'dd.mm.yyyy') || ' ' || p_inn || ' ' ||
                  p_snils || ' ' || to_char(SQLCODE) || ' ' ||
                  to_char(SQLERRM),
                  USER);
      found_debtors(-1) := null;
      RETURN found_debtors;
  END f_get_debtor_id;

  /*Функция определеяет является ли человек банкротом на дату p_by_date*/
  /*Возвращаемые значения:
  -2: не хватает данных для определения поиска в реестре банкротов.
  -1: ошибка при работе функции.
  0: клиент не найден в реестре банкротов.
  1: клиент находится в реестре банкротов,
            если это ЮЛ, то статус дел не определяется, с такими ЮЛ не работаем;
            если это ФЛ или ИП, то дело о банкротстве завершено.
  2: клиент (ИП или ФЛ) есть в реестре и имеет дело, которое находится в производстве, т.е. это текущий банкрот*/
  FUNCTION f_is_debtor(p_type      NUMBER /*1-ФЛ, 2-ЮЛ, 4-ИП*/,
                       p_inn       VARCHAR2,
                       p_snils     VARCHAR2,
                       p_ogrn      VARCHAR2,
                       p_birthdate DATE,
                       p_fullname  VARCHAR2) RETURN NUMBER IS
    found_debtors list_of_debtors;
    v_type        NUMBER := p_type;
    v_case_status NUMBER;
    v_res         NUMBER := 0;
  
  BEGIN
    IF debugging THEN
      DBMS_OUTPUT.PUT_LINE('Поиск в реестре банкротов');
    END IF;
    
    IF nvl(p_type, 0) NOT IN (1, 2, 4) THEN
      IF debugging THEN
        DBMS_OUTPUT.PUT_LINE('Не указан тип клиента, предполагаем, что это ЮЛ');
      END IF;
      v_type := 2;
    END IF;
  
    IF v_type = 1 AND p_birthdate IS NULL AND p_inn IS NULL AND
       p_snils IS NULL THEN
      p_log_efrsb(SYSDATE,
                  'E',
                  'F_IS_DEBTOR',
                  null,
                  null,
                  REGEXP_REPLACE('Клиент ' || p_fullname || ' ' ||
                                 to_char(p_birthdate, 'dd.mm.yyyy') || ' ' ||
                                 p_inn || ' ' || p_snils ||
                                 '. Для ФЛ необходимо ИНН или СНИЛС, иначе ФИО и дата рождения',
                                 '  *',
                                 ' '),
                  USER);
      v_res := -2;
      RETURN v_res;
    ELSIF v_type = 2 AND p_inn IS NULL AND p_ogrn IS NULL THEN
      p_log_efrsb(SYSDATE,
                  'E',
                  'F_IS_DEBTOR',
                  null,
                  null,
                  'Клиент ' || p_fullname ||
                  '. Для ЮЛ необходимо ИНН или ОГРН',
                  USER);
      v_res := -2;
      RETURN v_res;
    ELSIF v_type = 4 AND p_inn IS NULL AND p_ogrn IS NULL AND p_snils IS NULL THEN
      p_log_efrsb(SYSDATE,
                  'E',
                  'F_IS_DEBTOR',
                  null,
                  null,
                  'Клиент ' || p_fullname ||
                  '. Для ИП необходимо ИНН или ОГРН, или СНИЛС',
                  USER);
      v_res := -2;
      RETURN v_res;
    END IF;
  
    found_debtors := f_get_debtor_id(v_type,
                                     p_inn,
                                     p_snils,
                                     p_ogrn,
                                     p_birthdate,
                                     p_fullname);
  
    IF found_debtors.count > 0 THEN
      IF found_debtors.EXISTS(-1) THEN
        v_res := -1;
        RETURN v_res;
      ELSE
        FOR debt IN 1 .. found_debtors.LAST LOOP
          v_res := 1;
          p_get_legal_cases(found_debtors(debt), v_case_status);
        END LOOP;
      END IF;
    END IF;
  
    IF debugging AND v_res = 0 THEN
      DBMS_OUTPUT.PUT_LINE('Не найден в реестре банкротов');
    END IF;
  
    IF v_case_status = 1 THEN
      v_res := 2;
    ELSIF v_case_status = -1 THEN
      v_res := -1;
    END IF;
  
    RETURN v_res;
  EXCEPTION
    WHEN OTHERS THEN
      p_log_efrsb(SYSDATE,
                  'E',
                  'F_IS_DEBTOR',
                  null,
                  null,
                  to_char(SQLCODE) || ' ' || to_char(SQLERRM),
                  USER);
      v_res := -1;
      RETURN v_res;
  END f_is_debtor;

  /*Процедура массового логирования по найденным банкротам или по тем, у которых закончилось дело*/
  PROCEDURE p_log_efrsb_batch(p_collection IN t_found_cus_debtor_tab,
                              p_dml_type   IN VARCHAR2,
                              p_table      IN VARCHAR2,
                              p_errors     OUT VARCHAR2) IS
    v_text VARCHAR2(500);
  BEGIN
    IF p_dml_type = 'DELETE FROM' THEN
      v_text := 'Удаление';
    ELSIF p_dml_type = 'INSERT INTO' THEN
      v_text := 'Добавление';
    END IF;
    IF p_table = 'CUS_NOTE' THEN
      v_text := v_text || ' оповещения';
    ELSIF p_table = 'GCS' THEN
      v_text := v_text || ' категории-группы';
    END IF;
  
    FORALL indx IN p_collection.FIRST .. p_collection.LAST SAVE EXCEPTIONS
      INSERT INTO xxi.gis_efrsb_log
        (dtime, ctype, cfuncion, icusnum, ibankruptid, cmessage, cuser)
      VALUES
        (SYSDATE,
         'I',
         'P_NOTIF_PROCESSING + ' || p_table,
         p_collection(indx).iccusnum,
         p_collection(indx).debtorid,
         v_text,
         USER);
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      FOR indx IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
        p_log_efrsb(SYSDATE,
                    'E',
                    'P_LOG_EFRSB_BATCH',
                    p_collection       (SQL%BULK_EXCEPTIONS(indx).ERROR_INDEX).iccusnum,
                    p_collection       (SQL%BULK_EXCEPTIONS(indx).ERROR_INDEX).debtorid,
                    SQL%BULK_EXCEPTIONS(indx).ERROR_CODE,
                    USER);
      END LOOP;
    
      p_errors := p_errors + TO_NUMBER(SQL%BULK_EXCEPTIONS.COUNT);
  END p_log_efrsb_batch;

  /*Процедура создания динамического запроса на удаление или добавления категории-группы*/
  PROCEDURE p_cat_grp_processing(p_collection IN t_found_cus_debtor_tab,
                                 p_dml_type   IN VARCHAR2,
                                 p_dml_where  IN VARCHAR2,
                                 p_errors     OUT NUMBER) IS
    v_query VARCHAR2(300);
    v_cat   CONSTANT NUMBER := 18;
    v_group CONSTANT NUMBER := 4;
  BEGIN
    v_query := p_dml_type || ' GCS ' || p_dml_where;
    IF debugging THEN
      DBMS_OUTPUT.PUT_LINE(v_query);
      DBMS_OUTPUT.PUT_LINE(p_collection(p_collection.FIRST).iccusnum);
    END IF;
    FORALL indx IN p_collection.FIRST .. p_collection.LAST SAVE EXCEPTIONS
                                         EXECUTE IMMEDIATE v_query USING p_collection(indx).iccusnum,
                                         v_cat, v_group
      ;
    COMMIT;
  
    p_log_efrsb_batch(p_collection, p_dml_type, 'GCS', p_errors);
  
  EXCEPTION
    WHEN OTHERS THEN
      FOR indx IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
        p_log_efrsb(SYSDATE,
                    'E',
                    'P_CAT_GRP_PROCESSING + GCS',
                    null,
                    null,
                    'Клиент ' || p_collection(SQL%BULK_EXCEPTIONS(indx).ERROR_INDEX).iccusnum || ': ' || SQL%BULK_EXCEPTIONS(indx).ERROR_CODE,
                    USER);
      END LOOP;
      p_errors := p_errors + TO_NUMBER(SQL%BULK_EXCEPTIONS.COUNT);
  END p_cat_grp_processing;

  /*Процедура создания динамического запроса на удаление или добавления уведомления о банкротстве на клиенте*/
  PROCEDURE p_notif_processing(p_collection IN t_found_cus_debtor_tab,
                               p_dml_type   IN VARCHAR2,
                               p_dml_where  IN VARCHAR2,
                               p_errors     OUT VARCHAR2) IS
    v_query VARCHAR2(300);
  BEGIN
    v_query := p_dml_type || ' CUS_NOTE ' || p_dml_where;
    IF debugging THEN
      DBMS_OUTPUT.PUT_LINE(v_query);
      DBMS_OUTPUT.PUT_LINE(p_collection(p_collection.FIRST).iccusnum);
    END IF;
  
    FORALL indx IN p_collection.FIRST .. p_collection.LAST SAVE EXCEPTIONS
                                         EXECUTE IMMEDIATE v_query USING p_collection(indx).iccusnum,
                                         'Клиент признан банкротом. Ведётся судебное / внесудебное производство'
      ;
    COMMIT;
  
    p_log_efrsb_batch(p_collection, p_dml_type, 'CUS_NOTE', p_errors);
  
  EXCEPTION
    WHEN OTHERS THEN
      FOR indx IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
        p_log_efrsb(SYSDATE,
                    'E',
                    'P_NOTIF_PROCESSING + CUS_NOTE',
                    null,
                    null,
                    'Клиент ' || p_collection(SQL%BULK_EXCEPTIONS(indx).ERROR_INDEX).iccusnum || ': ' || SQL%BULK_EXCEPTIONS(indx).ERROR_CODE,
                    USER);
      END LOOP;
    
      p_errors := p_errors + TO_NUMBER(SQL%BULK_EXCEPTIONS.COUNT);
  END p_notif_processing;

  /*Поиск среди клиентов*/
  FUNCTION f_find_debtors RETURN VARCHAR2 IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    v_case_status  NUMBER;
    v_exists       NUMBER;
    v_loop_icusnum NUMBER;
    v_err_count    NUMBER := 0;
    found_debtors  list_of_debtors;
    return_row     t_found_cus_debtor;
  
    cus_all_debtors         t_found_cus_debtor_tab; /*найденные все банкроты среди клиентов */
    cus_cur_debtors         t_found_cus_debtor_tab; /*найденные банкроты с текущим делом */
    cus_prev_cur_debtors    t_found_cus_debtor_tab; /*прошлый результат банкротов-клиентов с текущим делом */
    cus_no_more_cur_debtors t_found_cus_debtor_tab; /*дело о банкротстве завершено, для удаления из оповещений и категории/группы */
    cus_new_cur_debtors     t_found_cus_debtor_tab; /*новый банкрот, для добавления оповещения и категории/группы */
  
    all_cus t_cus_record_col;
  
  BEGIN
    /*Отбираем тех, у кого признак 'Клиент', заполнено ИНН / СНИЛС / ОГРН или ФИО с датой рождения, ИНН <> ИНН ТКПБ*/
    SELECT ICUSNUM,
           CCUSFLAG,
           CCUSNAME,
           DCUSBIRTHDAY,
           CCUSNUMNAL,
           CCUSSNILS,
           CCUSKSIVA
      BULK COLLECT
      INTO all_cus
      FROM cus c
     WHERE nvl(c.CCUSNUMNAL, '.') <> '6829000028'
       AND c.ICUSSTATUS = 2
       AND ((case
              when c.CCUSFLAG = 1 and
                   (c.DCUSBIRTHDAY IS NOT NULL AND c.CCUSNAME IS NOT NULL) OR
                   (c.CCUSNUMNAL IS NOT NULL OR c.CCUSSNILS IS NOT NULL) THEN
               1
              ELSE
               0
            END) = 1 OR (case
              when c.CCUSFLAG <> 1 and
                   (c.CCUSKSIVA IS NOT NULL OR c.CCUSNUMNAL IS NOT NULL) THEN
               1
              ELSE
               0
            END) = 1);
  
    /*Отбираем активных банкротов ФЛ и просто ЮЛ с любым статусом дела с прошлой сверки*/
    SELECT d.ICUSNUM, BANKRUPTID, ISTATUS
      BULK COLLECT
      INTO cus_prev_cur_debtors
      FROM gis_cus_debtors d
      left join cus c
        on d.icusnum = c.icusnum
     WHERE (d.ISTATUS = 2 and c.ccusflag in (1, 4))
        OR c.ccusflag not in (1, 4);
  
    FOR indx IN all_cus.FIRST .. all_cus.LAST LOOP
      IF debugging THEN
        DBMS_OUTPUT.put_line(chr(10) || chr(10) || 'Клиент ' ||
                             to_char(all_cus(indx).icusnum));
      END IF;
      found_debtors := f_get_debtor_id(all_cus(indx).ccusflag,
                                       all_cus(indx).ccusnumnal,
                                       all_cus(indx).ccussnils,
                                       all_cus(indx).ccusksiva,
                                       all_cus(indx).DCUSBIRTHDAY,
                                       all_cus(indx).ccusname);
      IF found_debtors.count > 0 THEN
        IF found_debtors.EXISTS(-1) THEN
          IF debugging THEN
            DBMS_OUTPUT.put_line('Ошибка ' ||
                                 to_char(all_cus(indx).icusnum));
          END IF;
        ELSE
          FOR debt IN 1 .. found_debtors.LAST LOOP
            return_row.iccusnum := all_cus(indx).icusnum;
            return_row.debtorid := found_debtors(debt);
            p_get_legal_cases(return_row.debtorid, v_case_status);
            IF v_case_status IS NULL THEN
              return_row.status := 1;
              IF all_cus(indx).CCUSFLAG not in (1, 4) THEN
                cus_cur_debtors(cus_cur_debtors.COUNT + 1) := return_row;
              END IF;
            ELSIF v_case_status = 1 THEN
              return_row.status := 2;
              cus_cur_debtors(cus_cur_debtors.COUNT + 1) := return_row;
            ELSE
              return_row.status := -1;
            END IF;
            cus_all_debtors(cus_all_debtors.COUNT + 1) := return_row;
          END LOOP;
        END IF;
      END IF;
    END LOOP;
  
    /*Если первая загрузка, то Count = 0, поэтому идем дальше. Иначе: 1/2. Находим банкротов-клиентов, у которых завершилось дело о банкротстве*/
    IF cus_prev_cur_debtors.COUNT > 0 THEN
      FOR indx_p IN cus_prev_cur_debtors.FIRST .. cus_prev_cur_debtors.LAST LOOP
        v_exists       := 0;
        v_loop_icusnum := cus_prev_cur_debtors(indx_p).iccusnum;
        FOR indx_c IN cus_cur_debtors.FIRST .. cus_cur_debtors.LAST LOOP
          EXIT WHEN v_exists = 1;
          IF v_loop_icusnum = cus_cur_debtors(indx_c).iccusnum THEN
            /*IF debugging THEN DBMS_OUTPUT.PUT_LINE(v_loop_icusnum||' уже есть в таблице клиентах-банкротах'); /*END IF;*/
            v_exists := 1;
          END IF;
        END LOOP;
      
        IF v_exists = 0 THEN
          IF debugging THEN
            DBMS_OUTPUT.PUT_LINE('Завершено дело о банкротстве у ' ||
                                 v_loop_icusnum);
          END IF;
          cus_no_more_cur_debtors(cus_no_more_cur_debtors.COUNT + 1) := cus_prev_cur_debtors(indx_p);
        END IF;
      END LOOP;
    
      /*2/2. Находим новых актуальных банкротов-клиентов*/
      FOR indx_c IN cus_cur_debtors.FIRST .. cus_cur_debtors.LAST LOOP
        v_exists       := 0;
        v_loop_icusnum := cus_cur_debtors(indx_c).iccusnum;
        FOR indx_p IN cus_prev_cur_debtors.FIRST .. cus_prev_cur_debtors.LAST LOOP
          EXIT WHEN v_exists = 1;
          IF v_loop_icusnum = cus_prev_cur_debtors(indx_p).iccusnum THEN
            /*IF debugging THEN DBMS_OUTPUT.PUT_LINE('Уже есть в таблице клиентах-банкротах '||v_loop_icusnum); END IF;*/
            v_exists := 1;
          END IF;
        END LOOP;
      
        IF v_exists = 0 THEN
          IF debugging THEN
            DBMS_OUTPUT.PUT_LINE('Начато дело о банкротстве у ' ||
                                 v_loop_icusnum);
          END IF;
          cus_new_cur_debtors(cus_new_cur_debtors.COUNT + 1) := cus_cur_debtors(indx_c);
        END IF;
      END LOOP;
      /*При первой загрузке добавляем всем акутальным клиентам-банкротам уведомления и категорию-группу*/
    ELSIF cus_prev_cur_debtors.COUNT = 0 THEN
      p_notif_processing(cus_cur_debtors,
                         'INSERT INTO',
                         '(ICUSTOMER,  CTEXT) values (:1, :2)',
                         v_err_count);
      p_cat_grp_processing(cus_cur_debtors,
                           'INSERT INTO',
                           '(IGCSCUS,  IGCSCAT, IGCSNUM) values (:1, :2, :3)',
                           v_err_count);
    END IF;
  
    /*Если есть клиенты-банкроты, у которых завершилось производство, то убираем у них оповещение и категорию-группу*/
    IF cus_no_more_cur_debtors.COUNT > 0 THEN
      p_notif_processing(cus_no_more_cur_debtors,
                         'DELETE FROM',
                         'WHERE ICUSTOMER = :1 AND CTEXT = :2',
                         v_err_count);
      p_cat_grp_processing(cus_no_more_cur_debtors,
                           'DELETE FROM',
                           'WHERE IGCSCUS = :1 AND IGCSCAT = :2 AND IGCSNUM = :3',
                           v_err_count);
      IF debugging THEN
        FOR indx in cus_no_more_cur_debtors.FIRST .. cus_no_more_cur_debtors.LAST LOOP
          DBMS_OUTPUT.PUT_LINE('Удаляем оповещение и категорию-группу для ' || cus_no_more_cur_debtors(indx).iccusnum);
        END LOOP;
      END IF;
    ELSIF debugging THEN
      DBMS_OUTPUT.PUT_LINE('Нет текущих банкротов с завершенным производством');
    END IF;
  
    /*Если появился новый банкрот-клиент, то добавляем оповещение и категорию-группу*/
    IF cus_new_cur_debtors.COUNT > 0 THEN
      p_notif_processing(cus_new_cur_debtors,
                         'INSERT INTO',
                         '(ICUSTOMER,  CTEXT) values (:1, :2)',
                         v_err_count);
      p_cat_grp_processing(cus_new_cur_debtors,
                           'INSERT INTO',
                           '(IGCSCUS,  IGCSCAT, IGCSNUM) values (:1, :2, :3)',
                           v_err_count);
      IF debugging THEN
        FOR indx in cus_new_cur_debtors.FIRST .. cus_new_cur_debtors.LAST LOOP
          DBMS_OUTPUT.PUT_LINE('Добавляем оповещение и категорию-группу для ' || cus_new_cur_debtors(indx).iccusnum);
        END LOOP;
      END IF;
    ELSIF debugging THEN
      DBMS_OUTPUT.PUT_LINE('Нет новых банкротов');
    END IF;
  
    EXECUTE IMMEDIATE 'TRUNCATE TABLE gis_cus_debtors';
  
    /*Добавляем найденных банкротов со статусами 1 и 2 в таблицу gis_cus_debtors*/
    BEGIN
      FORALL indx IN 1 .. cus_all_debtors.COUNT SAVE EXCEPTIONS
        INSERT INTO gis_cus_debtors g
        VALUES
          (cus_all_debtors(indx).iccusnum,
           cus_all_debtors(indx).debtorid,
           cus_all_debtors(indx).status);
      COMMIT;
    EXCEPTION
      WHEN OTHERS THEN
        FOR indx IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
          p_log_efrsb(SYSDATE,
                      'E',
                      'F_FIND_DEBTORS + GIS_CUS_DEBTORS',
                      null,
                      null,
                      'Клиент ' || cus_all_debtors(SQL%BULK_EXCEPTIONS(indx).ERROR_INDEX).iccusnum || ': ' || SQL%BULK_EXCEPTIONS(indx).ERROR_CODE,
                      USER);
        END LOOP;
      
        v_err_count := v_err_count + TO_NUMBER(SQL%BULK_EXCEPTIONS.COUNT);
    END;
  
    RETURN 'Банкроты: ' || to_char(cus_all_debtors.COUNT) || ' . Активных: ' || to_char(cus_cur_debtors.COUNT) ||(case when
                                                                                                                  cus_new_cur_debtors.COUNT > 0 THEN
                                                                                                                  '. Новые банкроты: ' ||
                                                                                                                  cus_new_cur_debtors.COUNT else '' end) ||(case when
                                                                                                                                                            cus_no_more_cur_debtors.COUNT > 0 THEN
                                                                                                                                                            '. Удаленные банкроты: ' ||
                                                                                                                                                            cus_no_more_cur_debtors.COUNT else '' end) ||(case when
                                                                                                                                                                                                          v_err_count = 0 OR
                                                                                                                                                                                                          v_err_count IS NULL then '' else
                                                                                                                                                                                                          '. Ошибок: ' ||
                                                                                                                                                                                                          to_char(v_err_count) end);
  END f_find_debtors;

  FUNCTION f_get_leg_cases(p_debtor_id NUMBER) RETURN list_of_cases PIPELINED IS
      
  v_case   VARCHAR2(300);
  leg_case SYS_REFCURSOR;
  
  BEGIN
    OPEN leg_case FOR
      SELECT CASENUMBER || ' ' || COURT
        FROM gis_efrsb_cases
       WHERE BANKRUPTID = p_debtor_id;
    LOOP
      FETCH leg_case
        INTO v_case;
      IF leg_case%ROWCOUNT = 0 THEN
        PIPE ROW('Отсутствуют судебные дела');
      END IF;
      EXIT WHEN leg_case%NOTFOUND;
      IF leg_case%ROWCOUNT >= 1 THEN
        PIPE ROW(v_case);
      END IF;
    END LOOP;
    CLOSE leg_case;
    RETURN;
  END f_get_leg_cases;
END;
/
