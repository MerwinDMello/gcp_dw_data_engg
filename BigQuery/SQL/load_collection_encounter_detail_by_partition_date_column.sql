DECLARE truncate_stmt, insert_stmt, select_stmt STRING;
DECLARE reporting_date_list ARRAY<DATE>;
DECLARE table_list ARRAY<STRING>;

BEGIN

SET table_list = ['collection_encounter_detail'];

FOR rec1 IN
(SELECT * from UNNEST(table_list) as table_name)
DO

  SET truncate_stmt = (SELECT FORMAT( '''
  TRUNCATE TABLE edwpbs.%s;''', rec1.table_name));

  EXECUTE IMMEDIATE truncate_stmt;

  SET select_stmt = (SELECT FORMAT( '''
  SELECT ARRAY_AGG(DISTINCT reporting_date ORDER BY reporting_date DESC) AS reporting_date_list FROM edwpbs_base_views_copy.%s;''', rec1.table_name));

  EXECUTE IMMEDIATE select_stmt INTO reporting_date_list;

  BEGIN TRANSACTION;

  FOR rec2 IN
  (SELECT * from UNNEST(reporting_date_list) as reporting_date)
  DO

  SET insert_stmt = (SELECT FORMAT( '''
  INSERT INTO edwpbs.%s
  SELECT * FROM `edwpbs_base_views_copy.%s` WHERE reporting_date = '%t';''', rec1.table_name, rec1.table_name, rec2.reporting_date));

  EXECUTE IMMEDIATE insert_stmt;

  END FOR;

  COMMIT TRANSACTION;

END FOR;

END;