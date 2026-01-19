DECLARE truncate_stmt, insert_stmt, select_stmt STRING;
DECLARE code_value_list ARRAY<STRING>;
DECLARE table_list ARRAY<STRING>;

BEGIN

SET table_list = ['fact_code'];

FOR rec1 IN
(SELECT * from UNNEST(table_list) as table_name)
DO

  SET truncate_stmt = (SELECT FORMAT( '''
  TRUNCATE TABLE {{ params.param_clm_core_dataset_name }}.%s;''', rec1.table_name));

  EXECUTE IMMEDIATE truncate_stmt;

  SET select_stmt = (SELECT FORMAT( '''
  SELECT ARRAY_AGG(DISTINCT COALESCE(TRIM(SUBSTR(code_value,1,2)), 'ZZ') ORDER BY COALESCE(TRIM(SUBSTR(code_value,1,2)), 'ZZ') DESC) AS code_value_list FROM {{ params.param_clm_mirrored_base_views_dataset_name }}.%s;''', rec1.table_name));

  EXECUTE IMMEDIATE select_stmt INTO code_value_list;

  FOR rec2 IN
  (SELECT * from UNNEST(code_value_list) as code_value)
  DO

  SET insert_stmt = (SELECT FORMAT( '''
  INSERT INTO {{ params.param_clm_core_dataset_name }}.%s
  SELECT * FROM `{{ params.param_clm_mirrored_base_views_dataset_name }}.%s` WHERE COALESCE(TRIM(SUBSTR(code_value,1,2)), 'ZZ') = '%s';''', rec1.table_name, rec1.table_name, rec2.code_value));

  BEGIN TRANSACTION;

  EXECUTE IMMEDIATE insert_stmt;

  COMMIT TRANSACTION;

  END FOR;

END FOR;

END;