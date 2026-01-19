DECLARE truncate_stmt, insert_stmt, select_stmt STRING;
DECLARE charge_revenue_code_list ARRAY<STRING>;
DECLARE table_list ARRAY<STRING>;

BEGIN

SET table_list = ['fact_charge'];

FOR rec1 IN
(SELECT * from UNNEST(table_list) as table_name)
DO

  SET truncate_stmt = (SELECT FORMAT( '''
  TRUNCATE TABLE {{ params.param_clm_core_dataset_name }}.%s;''', rec1.table_name));

  EXECUTE IMMEDIATE truncate_stmt;

  SET select_stmt = (SELECT FORMAT( '''
  SELECT ARRAY_AGG(DISTINCT COALESCE(TRIM(charge_revenue_code), 'XXXX') ORDER BY COALESCE(TRIM(charge_revenue_code), 'XXXX') DESC) AS charge_revenue_code_list FROM {{ params.param_clm_mirrored_base_views_dataset_name }}.%s;''', rec1.table_name));

  EXECUTE IMMEDIATE select_stmt INTO charge_revenue_code_list;

  FOR rec2 IN
  (SELECT * from UNNEST(charge_revenue_code_list) as charge_revenue_code)
  DO

  SET insert_stmt = (SELECT FORMAT( '''
  INSERT INTO {{ params.param_clm_core_dataset_name }}.%s
  SELECT * FROM `{{ params.param_clm_mirrored_base_views_dataset_name }}.%s` WHERE COALESCE(TRIM(charge_revenue_code), 'XXXX') = '%s';''', rec1.table_name, rec1.table_name, rec2.charge_revenue_code));

  BEGIN TRANSACTION;

  EXECUTE IMMEDIATE insert_stmt;

  COMMIT TRANSACTION;

  END FOR;

END FOR;

END;