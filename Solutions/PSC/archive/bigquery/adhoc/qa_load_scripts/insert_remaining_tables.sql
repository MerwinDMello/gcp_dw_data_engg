DECLARE dataset_list ARRAY<STRING>;
DECLARE table_name_list ARRAY<STRING>;
DECLARE select_stmt STRING;
DECLARE insert_stmt STRING;

SET dataset_list = ['edwpsc']; 

-- Execute Loop for each dataset in the list
FOR rec1 IN ( 
  SELECT * FROM UNNEST(dataset_list) AS dataset_name) 
DO 

-- Get the list of tables in the dataset
  SET select_stmt = (SELECT FORMAT( ''' SELECT ARRAY_AGG(DISTINCT table_name ORDER BY table_name ASC) AS table_name_list FROM %s.INFORMATION_SCHEMA.TABLES WHERE table_type = 'BASE TABLE' AND UPPER(TRIM(table_name)) NOT IN ('HIN_SECREF_FACILITY', 'ARTIVA_STGGCPOOLGENINFO_HISTORY')  ''', rec1.dataset_name));

  EXECUTE IMMEDIATE select_stmt INTO table_name_list;
-- Execute Loop for each table in the dataset
  FOR rec2 IN
  (SELECT * from UNNEST(table_name_list) as table_name)
  
  DO
-- Transaction is used if there is a large number of tables in the dataset  
	BEGIN TRANSACTION;

-- Empty the table using a truncate  
    EXECUTE IMMEDIATE FORMAT("TRUNCATE TABLE %s.%s", rec1.dataset_name, rec2.table_name);

-- Execute Insert Select to load the records
    SET insert_stmt = (SELECT FORMAT( '''
    INSERT INTO %s.%s
    SELECT * FROM `prod_support.%s`;''', 
    rec1.dataset_name, rec2.table_name, rec2.table_name));

    EXECUTE IMMEDIATE insert_stmt;
	
	COMMIT TRANSACTION;

  END FOR;

END FOR;