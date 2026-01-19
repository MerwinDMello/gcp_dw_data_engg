BEGIN
BEGIN TRANSACTION;

	 -- Truncate Ingest Working Table for Write Off
	 TRUNCATE TABLE {core}.ref_lookup_name;

	 -- Populate Ingest Working Table for Write Off
	 INSERT INTO {core}.ref_lookup_name
	 (lookup_sid, lookup_name, source_system_code,dw_last_update_date_time)
     SELECT 
	 rlns.lookup_sid,
	 rlns.lookup_name,
	 rlns.source_system_code,
	 datetime_trunc(current_datetime('US/Central'), SECOND)
     FROM {stage}.ref_lookup_name_stg rlns
;
COMMIT TRANSACTION;
END;