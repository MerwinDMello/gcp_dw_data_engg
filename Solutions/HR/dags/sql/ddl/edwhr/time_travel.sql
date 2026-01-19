CREATE OR REPLACE PROCEDURE `{{ params.param_hr_core_dataset_name }}.time_travel`(ds_name STRING, tbl_name STRING, expected_date STRING, expected_time STRING)
BEGIN

EXECUTE IMMEDIATE CONCAT("CREATE TEMP TABLE ", ds_name, "_", tbl_name, "_temp AS ", 
"Select * from ",  ds_name, ".", tbl_name, " ",
"FOR SYSTEM_TIME AS OF TIMESTAMP(DATETIME('", expected_date, "', TIME '", expected_time, "'),'US/Central');"
);

EXECUTE IMMEDIATE CONCAT( "TRUNCATE TABLE ", ds_name, ".", tbl_name, ";"
);

EXECUTE IMMEDIATE CONCAT( "INSERT INTO ", ds_name, ".", tbl_name, " ",
"SELECT * FROM ", ds_name, "_", tbl_name, "_temp;"
);

END;