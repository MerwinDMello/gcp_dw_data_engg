DECLARE dataset_list ARRAY<STRING>;
DECLARE table_name_list ARRAY<STRING>;
DECLARE create_stmt STRING;
DECLARE alter_stmt STRING;
DECLARE select_stmt STRING;
DECLARE insert_stmt STRING;
DECLARE curr_date STRING;

SET dataset_list = ['edwpsc']; 

EXECUTE IMMEDIATE "SELECT FORMAT_DATE('%Y_%m_%d', CURRENT_DATE('US/Central'))" INTO curr_date;

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

-- Drop the prod support table in case it is present  
    EXECUTE IMMEDIATE FORMAT("DROP TABLE IF EXISTS `prod_support.%s_%s`", rec2.table_name, curr_date);

-- Create the prod support table
    SET create_stmt = (SELECT FORMAT( '''
    CREATE TABLE `prod_support.%s_%s`
    AS
    SELECT * FROM `%s.%s`
    FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(TIMESTAMP(CURRENT_DATETIME("US/Central")), INTERVAL 1 DAY);''', 
    rec2.table_name, curr_date, rec1.dataset_name, rec2.table_name));

    EXECUTE IMMEDIATE create_stmt;

-- Set Expiration Date after two weeks
    SET alter_stmt = (SELECT FORMAT( '''
    ALTER TABLE `prod_support.%s_%s`
    SET OPTIONS (expiration_timestamp = TIMESTAMP_ADD(TIMESTAMP(CURRENT_DATETIME("US/Central")), INTERVAL 14 DAY));''', 
    rec2.table_name, curr_date));

    EXECUTE IMMEDIATE alter_stmt;

-- Transaction is used if there is a large number of tables in the dataset  
	BEGIN TRANSACTION;
-- Empty the table using a truncate  
    EXECUTE IMMEDIATE FORMAT("TRUNCATE TABLE %s.%s", rec1.dataset_name, rec2.table_name);

-- Execute Insert Select to load the records
    SET insert_stmt = (SELECT FORMAT( '''
    INSERT INTO %s.%s
    SELECT * FROM `prod_support.%s_%s`;''', 
    rec1.dataset_name, rec2.table_name, rec2.table_name, curr_date));

    EXECUTE IMMEDIATE insert_stmt;
	
	COMMIT TRANSACTION;

  END FOR;

END FOR;