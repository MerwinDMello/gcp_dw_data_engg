DECLARE dataset_list ARRAY<STRING>;
DECLARE table_name_list ARRAY<STRING>;
DECLARE select_stmt STRING;

SET dataset_list = ['{{ params.param_im_stage_dataset_name }}','{{ params.param_im_core_dataset_name }}','{{ params.param_im_audit_dataset_name }}']; 

FOR rec1 IN ( 
  SELECT * FROM UNNEST(dataset_list) AS dataset_name) 
DO 

  SET select_stmt = (SELECT FORMAT( ''' SELECT ARRAY_AGG(DISTINCT table_name ORDER BY table_name DESC) AS table_name_list FROM %s.INFORMATION_SCHEMA.TABLES WHERE table_type = 'BASE TABLE' AND UPPER(TRIM(table_name)) <> 'HIN_SECREF_FACILITY' ''', rec1.dataset_name));

  EXECUTE IMMEDIATE select_stmt INTO table_name_list;

  FOR rec2 IN
  (SELECT * from UNNEST(table_name_list) as table_name)
  
  DO
    EXECUTE IMMEDIATE FORMAT("DROP TABLE %s.%s", rec1.dataset_name, rec2.table_name);
  END FOR;

END FOR;