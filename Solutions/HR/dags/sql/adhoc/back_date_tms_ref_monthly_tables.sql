DECLARE delete_stmt STRING;
DECLARE process_date STRING;
DECLARE table_list ARRAY<STRING>;
SET process_date = '2023-06-01';
SET table_list = ['ref_competency','ref_competency_group'];
-- BEGIN TRANSACTION;
FOR rec1 IN
(SELECT * from UNNEST(table_list) as table_name)
DO

	SET delete_stmt = (SELECT FORMAT( '''
	DELETE FROM `{{ params.param_hr_core_dataset_name }}`.%s
	WHERE DATE(dw_last_update_date_time) >= '%s';''', rec1.table_name, process_date));

	EXECUTE IMMEDIATE delete_stmt;
	
END FOR;
-- COMMIT TRANSACTION;