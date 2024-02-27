CREATE OR REPLACE PACKAGE gis_efrsb_util AS

  TYPE debtor_info_t IS RECORD(
    lnm   gis_efrsb_debtors.lastname%TYPE,
    fn    gis_efrsb_debtors.firstname%TYPE,
    pn    gis_efrsb_debtors.patronymicname%TYPE,
    fulln gis_efrsb_debtors.fullname%TYPE,
    inn   gis_efrsb_debtors.inn%TYPE,
    snils gis_efrsb_debtors.snils%TYPE,
    ogrn  gis_efrsb_debtors.ogrn%TYPE,
    reg   gis_efrsb_debtors.region%TYPE,
    addr  gis_efrsb_debtors.address%TYPE,
    bd    gis_efrsb_debtors.birthdate%TYPE,
    bp    gis_efrsb_debtors.birthplace%TYPE);

  debugging BOOLEAN := FALSE;

  PROCEDURE set_debugging(p_debugging BOOLEAN);

  PROCEDURE construst_set_query(p_set IN OUT VARCHAR2, p_type IN VARCHAR2, p_column IN VARCHAR2, p_string IN VARCHAR2 := null, p_date IN DATE := null , p_number IN NUMBER := null);

  PROCEDURE compare_records(debtor_id IN NUMBER,
                            p_new     IN debtor_info_t,
                            p_old     IN debtor_info_t,
                            p_change  OUT NUMBER);

  FUNCTION check_duplicate(p_id         NUMBER,
                           p_lastname   VARCHAR2,
                           p_firstname  VARCHAR2,
                           p_patrname   VARCHAR2,
                           p_fullname   VARCHAR2,
                           p_inn        VARCHAR2,
                           p_snils      VARCHAR2,
                           p_ogrn       VARCHAR2,
                           p_region     VARCHAR2,
                           p_address    VARCHAR2,
                           b_birthdate  DATE,
                           p_birthplace VARCHAR2) RETURN VARCHAR2;

END;
/
CREATE OR REPLACE PACKAGE BODY gis_efrsb_util AS
  PROCEDURE set_debugging(p_debugging BOOLEAN) IS
  BEGIN
      debugging := p_debugging;
  END set_debugging;

  PROCEDURE construst_set_query(p_set    IN OUT VARCHAR2,
                                p_type   IN VARCHAR2,
                                p_column IN VARCHAR2,
                                p_string IN VARCHAR2 := null,
                                p_date   IN DATE := null,
                                p_number IN NUMBER := null) IS
  BEGIN
    IF p_type = 'STRING' THEN
      IF p_string IS NOT NULL THEN
        p_set := p_set || p_column || ' = ''' || p_string || ''', ';
      ELSE
        p_set := p_set || p_column || ' = null, ';
      END IF;
    END IF;
    IF p_type = 'DATE' THEN
      IF p_date IS NOT NULL THEN
        p_set := p_set || p_column || q'{ = to_date('}' ||
                 to_char(p_date, 'dd.mm.yyyy') || q'{', 'dd.mm.yyyy')}' || ', ';
      ELSE
        p_set := p_set || p_column || ' = null, ';
      END IF;
    END IF;
    IF p_type = 'NUMBER' THEN
      IF p_number IS NOT NULL THEN
        p_set := p_set || p_column || q'{ = to_number('}' ||
                 to_char(p_number) || q'{)}' || ', ';
      ELSE
        p_set := p_set || p_column || ' = null, ';
      END IF;
    END IF;
  END construst_set_query;

  PROCEDURE compare_records(debtor_id IN NUMBER,
                            p_new     IN debtor_info_t,
                            p_old     IN debtor_info_t,
                            p_change  OUT NUMBER) IS
    PRAGMA AUTONOMOUS_TRANSACTION;

    v_query       VARCHAR2(4000);
    v_query_start VARCHAR2(50) := 'UPDATE gis_efrsb_debtors ';
    v_query_set   VARCHAR2(3900) := 'SET ';
    v_query_end   VARCHAR2(50) := ' WHERE BANKRUPTID = :val1';

  BEGIN
    IF nvl(p_new.lnm, '.') <> nvl(p_old.lnm, '.') THEN
      IF debugging THEN dbms_output.put_line('lnm'); END IF;
      construst_set_query(p_set    => v_query_set,
                          p_type   => 'STRING',
                          p_column => 'LASTNAME',
                          p_string => p_new.lnm);
      p_change := 1;
    END IF;
    IF nvl(p_new.fn, '.') <> nvl(p_old.fn, '.') THEN
      IF debugging THEN dbms_output.put_line('fn'); END IF;
      construst_set_query(p_set    => v_query_set,
                          p_type   => 'STRING',
                          p_column => 'FIRSTNAME',
                          p_string => p_new.fn);
      p_change := 1;
    END IF;
    IF nvl(p_new.pn, '.') <> nvl(p_old.pn, '.') THEN
      IF debugging THEN dbms_output.put_line('pn'); END IF;
      construst_set_query(p_set    => v_query_set,
                          p_type   => 'STRING',
                          p_column => 'PATRONYMICNAME',
                          p_string => p_new.pn);
      p_change := 1;
    END IF;
    IF nvl(p_new.fulln, '.') <> nvl(p_old.fulln, '.') THEN
      IF debugging THEN dbms_output.put_line('fulln'); END IF;
      construst_set_query(p_set    => v_query_set,
                          p_type   => 'STRING',
                          p_column => 'FULLNAME',
                          p_string => p_new.fulln);
      p_change := 1;
    END IF;
    IF nvl(p_new.inn, '.') <> nvl(p_old.inn, '.') THEN
      IF debugging THEN dbms_output.put_line('inn'); END IF;
      construst_set_query(p_set    => v_query_set,
                          p_type   => 'STRING',
                          p_column => 'INN',
                          p_string => p_new.inn);
      p_change := 1;
    END IF;
    IF nvl(p_new.snils, '.') <> nvl(p_old.snils, '.') THEN
      IF debugging THEN dbms_output.put_line('snils'); END IF;
      construst_set_query(p_set    => v_query_set,
                          p_type   => 'STRING',
                          p_column => 'SNILS',
                          p_string => p_new.snils);
      p_change := 1;
    END IF;
    IF nvl(p_new.ogrn, '.') <> nvl(p_old.ogrn, '.') THEN
      IF debugging THEN dbms_output.put_line('ogrn'); END IF;
      construst_set_query(p_set    => v_query_set,
                          p_type   => 'STRING',
                          p_column => 'OGRN',
                          p_string => p_new.ogrn);
      p_change := 1;
    END IF;
    IF nvl(p_new.reg, '.') <> nvl(p_old.reg, '.') THEN
      IF debugging THEN dbms_output.put_line('reg'); END IF;
      construst_set_query(p_set    => v_query_set,
                          p_type   => 'STRING',
                          p_column => 'REGION',
                          p_string => p_new.reg);
      p_change := 1;
    END IF;
    IF nvl(p_new.addr, '.') <> nvl(p_old.addr, '.') THEN
      IF debugging THEN dbms_output.put_line('addr'); END IF;
      construst_set_query(p_set    => v_query_set,
                          p_type   => 'STRING',
                          p_column => 'ADDRESS',
                          p_string => p_new.addr);
      p_change := 1;
    END IF;
    IF nvl(p_new.bd, to_date('01.01.1900', 'dd.mm.yyyy')) <>
       nvl(p_old.bd, to_date('01.01.1900', 'dd.mm.yyyy')) THEN
      IF debugging THEN dbms_output.put_line('bd'); END IF;
      construst_set_query(p_set    => v_query_set,
                          p_type   => 'DATE',
                          p_column => 'BIRTHDATE',
                          p_date   => p_new.bd);
      p_change := 1;
    END IF;
    IF nvl(p_new.bp, '.') <> nvl(p_old.bp, '.') THEN
      IF debugging THEN dbms_output.put_line('bp'); END IF;
      construst_set_query(p_set    => v_query_set,
                          p_type   => 'STRING',
                          p_column => 'BIRTHPLACE',
                          p_string => p_new.bp);
      p_change := 1;
    END IF;
    IF p_change = 1 THEN
      v_query := v_query_start ||
                 SUBSTR(v_query_set, 1, LENGTH(v_query_set) - 2) ||
                 v_query_end;
      IF debugging THEN dbms_output.put_line(v_query); END IF;
      EXECUTE IMMEDIATE v_query
        USING debtor_id;
      COMMIT;
    END IF;
  END compare_records;

  PROCEDURE populate_new_rec(p_new_rec    IN OUT debtor_info_t,
                             p_lastname   IN VARCHAR2,
                             p_firstname  IN VARCHAR2,
                             p_patrname   IN VARCHAR2,
                             p_fullname   IN VARCHAR2,
                             p_inn        IN VARCHAR2,
                             p_snils      IN VARCHAR2,
                             p_ogrn       IN VARCHAR2,
                             p_region     IN VARCHAR2,
                             p_address    IN VARCHAR2,
                             b_birthdate  IN DATE,
                             p_birthplace IN VARCHAR2) IS
  BEGIN
    p_new_rec.lnm   := p_lastname;
    p_new_rec.fn    := p_firstname;
    p_new_rec.pn    := p_patrname;
    p_new_rec.fulln := p_fullname;
    p_new_rec.inn   := p_inn;
    p_new_rec.snils := p_snils;
    p_new_rec.ogrn  := p_ogrn;
    p_new_rec.reg   := p_region;
    p_new_rec.addr  := p_address;
    p_new_rec.bd    := b_birthdate;
    p_new_rec.bp    := p_birthplace;
  END populate_new_rec;

  FUNCTION check_duplicate(p_id         NUMBER,
                           p_lastname   VARCHAR2,
                           p_firstname  VARCHAR2,
                           p_patrname   VARCHAR2,
                           p_fullname   VARCHAR2,
                           p_inn        VARCHAR2,
                           p_snils      VARCHAR2,
                           p_ogrn       VARCHAR2,
                           p_region     VARCHAR2,
                           p_address    VARCHAR2,
                           b_birthdate  DATE,
                           p_birthplace VARCHAR2) RETURN VARCHAR2 IS

    new_record debtor_info_t;
    cur_record debtor_info_t;
    v_change   NUMBER;
  BEGIN
    populate_new_rec(new_record,
                     p_lastname,
                     p_firstname,
                     p_patrname,
                     p_fullname,
                     p_inn,
                     p_snils,
                     p_ogrn,
                     p_region,
                     p_address,
                     b_birthdate,
                     p_birthplace);

    SELECT lastname,
           firstname,
           patronymicname,
           fullname,
           inn,
           snils,
           ogrn,
           region,
           address,
           birthdate,
           birthplace
      INTO cur_record
      FROM gis_efrsb_debtors
     WHERE bankruptid = p_id;

    compare_records(p_id, new_record, cur_record, v_change);

    IF v_change = 1 THEN
      RETURN 'Success';
    ELSE
      RETURN 'No changes';
    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      RETURN to_char(SQLCODE) || ' ' || to_char(SQLERRM);
  END check_duplicate;
END;
/
