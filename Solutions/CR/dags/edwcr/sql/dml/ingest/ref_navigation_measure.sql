BEGIN
BEGIN TRANSACTION;

	 -- Truncate Ingest Working Table for Write Off
	 TRUNCATE TABLE {core}.ref_navigation_measure;

	 -- Populate Ingest Working Table for Write Off
	 INSERT INTO {core}.ref_navigation_measure
	 (nav_measure_id, nav_measure_type, nav_measure_name,source_system_code,dw_last_update_date_time)
     SELECT 
	  rfms.nav_measure_id,
	  rfms.nav_measure_type,
	  rfms.nav_measure_name,
	  rfms.source_system_code,
	 datetime_trunc(current_datetime('US/Central'), SECOND)
     FROM {stage}.ref_navigation_measure_stg rfms
;
COMMIT TRANSACTION;
END;
