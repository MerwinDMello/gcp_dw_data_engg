DECLARE truncate_stmt, insert_stmt, delete_stmt, update_stmt STRING;

DECLARE process_date_list ARRAY<STRING>;
DECLARE table_list ARRAY<STRING>;

SET process_date_list = ['2023-07-31','2023-07-01','2023-06-01'];
SET table_list = ['employee_competency_detail'];

BEGIN TRANSACTION;

FOR rec1 IN
(SELECT * from UNNEST(table_list) as table_name)
DO

  SET truncate_stmt = (SELECT FORMAT( '''
  TRUNCATE TABLE prod_support.%s;''', rec1.table_name));

  EXECUTE IMMEDIATE truncate_stmt;

  SET insert_stmt = (SELECT FORMAT( '''
  INSERT INTO prod_support.%s
  SELECT * FROM `edwhr.%s`;''', rec1.table_name, rec1.table_name));

  EXECUTE IMMEDIATE insert_stmt;

  FOR rec2 IN
  (SELECT * from UNNEST(process_date_list) as process_date)
  DO

    SET delete_stmt = (SELECT FORMAT( '''
    DELETE FROM `prod_support.%s`
    WHERE DATE(VALID_FROM_DATE) = '%s'
    AND VALID_TO_DATE = DATETIME('9999-12-31 23:59:59');''', rec1.table_name, rec2.process_date));

    EXECUTE IMMEDIATE delete_stmt;

    SET update_stmt = (SELECT FORMAT( '''
    UPDATE `prod_support.%s`
    SET VALID_TO_DATE = DATETIME('9999-12-31 23:59:59')
    WHERE DATE(VALID_TO_DATE) = '%s';''', rec1.table_name, rec2.process_date));

    EXECUTE IMMEDIATE update_stmt;

  END FOR;

END FOR;

COMMIT TRANSACTION;