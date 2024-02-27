CREATE OR REPLACE TRIGGER efrsb_debtor_info_hist
       before update on gis_efrsb_debtors
       for each row
begin
  IF nvl(:old.LASTNAME, '.') <> nvl(:new.LASTNAME, '.') THEN
    INSERT INTO GIS_EFRSB_DEBTOR_HISTORY VALUES (GIS_EFRSB_DEBTOR_HISTORY_SEQ.nextval, :old.bankruptid, 'LASTNAME', :old.LASTNAME, :new.LASTNAME, SYSDATE);
  END IF;

  IF nvl(:old.FIRSTNAME, '.') <> nvl(:new.FIRSTNAME, '.') THEN
    INSERT INTO GIS_EFRSB_DEBTOR_HISTORY VALUES (GIS_EFRSB_DEBTOR_HISTORY_SEQ.nextval, :old.bankruptid, 'FIRSTNAME', :old.FIRSTNAME, :new.FIRSTNAME, SYSDATE);
  END IF;

  IF nvl(:old.PATRONYMICNAME, '.') <> nvl(:new.PATRONYMICNAME, '.') THEN
    INSERT INTO GIS_EFRSB_DEBTOR_HISTORY VALUES (GIS_EFRSB_DEBTOR_HISTORY_SEQ.nextval, :old.bankruptid, 'PATRONYMICNAME', :old.PATRONYMICNAME, :new.PATRONYMICNAME, SYSDATE);
  END IF;

  IF nvl(:old.FULLNAME, '.') <> nvl(:new.FULLNAME, '.') THEN
    INSERT INTO GIS_EFRSB_DEBTOR_HISTORY VALUES (GIS_EFRSB_DEBTOR_HISTORY_SEQ.nextval, :old.bankruptid, 'FULLNAME', :old.FULLNAME, :new.FULLNAME, SYSDATE);
  END IF;

  IF nvl(:old.INN, '.') <> nvl(:new.INN, '.') THEN
    INSERT INTO GIS_EFRSB_DEBTOR_HISTORY VALUES (GIS_EFRSB_DEBTOR_HISTORY_SEQ.nextval, :old.bankruptid, 'INN', :old.INN, :new.INN, SYSDATE);
  END IF;

  IF nvl(:old.SNILS, '.') <> nvl(:new.SNILS, '.') THEN
    INSERT INTO GIS_EFRSB_DEBTOR_HISTORY VALUES (GIS_EFRSB_DEBTOR_HISTORY_SEQ.nextval, :old.bankruptid, 'SNILS', :old.SNILS, :new.SNILS, SYSDATE);
  END IF;

  IF nvl(:old.OGRN, '.') <> nvl(:new.OGRN, '.') THEN
    INSERT INTO GIS_EFRSB_DEBTOR_HISTORY VALUES (GIS_EFRSB_DEBTOR_HISTORY_SEQ.nextval, :old.bankruptid, 'OGRN', :old.OGRN, :new.OGRN, SYSDATE);
  END IF;

  IF nvl(:old.REGION, '.') <> nvl(:new.REGION, '.') THEN
    INSERT INTO GIS_EFRSB_DEBTOR_HISTORY VALUES (GIS_EFRSB_DEBTOR_HISTORY_SEQ.nextval, :old.bankruptid, 'REGION', :old.REGION, :new.REGION, SYSDATE);
  END IF;

  IF nvl(:old.ADDRESS, '.') <> nvl(:new.ADDRESS, '.') THEN
    INSERT INTO GIS_EFRSB_DEBTOR_HISTORY VALUES (GIS_EFRSB_DEBTOR_HISTORY_SEQ.nextval, :old.bankruptid, 'ADDRESS', :old.ADDRESS, :new.ADDRESS, SYSDATE);
  END IF;

  IF nvl(:old.BIRTHDATE, to_date('01.01.1900', 'dd.mm.yyyy')) <> nvl(:new.BIRTHDATE, to_date('01.01.1900', 'dd.mm.yyyy')) THEN
    INSERT INTO GIS_EFRSB_DEBTOR_HISTORY VALUES (GIS_EFRSB_DEBTOR_HISTORY_SEQ.nextval, :old.bankruptid, 'BIRTHDATE', decode(:old.BIRTHDATE, null, null, to_char(:old.BIRTHDATE, 'dd.mm.yyyy')), decode(:new.BIRTHDATE, null, null, to_char(:new.BIRTHDATE, 'dd.mm.yyyy')), SYSDATE);
  END IF;

  IF nvl(:old.BIRTHPLACE, '.') <> nvl(:new.BIRTHPLACE, '.') THEN
    INSERT INTO GIS_EFRSB_DEBTOR_HISTORY VALUES (GIS_EFRSB_DEBTOR_HISTORY_SEQ.nextval, :old.bankruptid, 'BIRTHPLACE', :old.BIRTHPLACE, :new.BIRTHPLACE, SYSDATE);
  END IF;

end;
