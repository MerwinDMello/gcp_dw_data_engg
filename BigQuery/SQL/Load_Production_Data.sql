DECLARE dataset_list ARRAY<STRING>;
DECLARE table_name_list ARRAY<STRING>;
DECLARE select_stmt STRING;
DECLARE insert_stmt STRING;

SET dataset_list = ['edwim']; 

FOR rec1 IN ( 
  SELECT * FROM UNNEST(dataset_list) AS dataset_name) 
DO 

  SET select_stmt = (SELECT FORMAT( ''' SELECT ARRAY_AGG(DISTINCT table_name ORDER BY table_name ASC) AS table_name_list FROM %s.INFORMATION_SCHEMA.TABLES WHERE table_type = 'BASE TABLE' AND UPPER(TRIM(table_name)) NOT IN ('HIN_SECREF_FACILITY', 'PROVIDER_PRIVILEGE') AND NOT ENDS_WITH(UPPER(TRIM(table_name)),'_BKP') AND NOT ENDS_WITH(UPPER(TRIM(table_name)),'_TEST')  ''', rec1.dataset_name));

  EXECUTE IMMEDIATE select_stmt INTO table_name_list;

  FOR rec2 IN
  (SELECT * from UNNEST(table_name_list) as table_name)
  
  DO
  
	BEGIN TRANSACTION;
  
    EXECUTE IMMEDIATE FORMAT("TRUNCATE TABLE %s.%s", rec1.dataset_name, rec2.table_name);

    SET insert_stmt = (SELECT FORMAT( '''
    INSERT INTO %s.%s
    SELECT * FROM `hca-hin-prod-cur-comp.%s.%s`;''', 
    rec1.dataset_name, rec2.table_name, rec1.dataset_name, rec2.table_name));

    EXECUTE IMMEDIATE insert_stmt;
	
	COMMIT TRANSACTION;

  END FOR;

END FOR;