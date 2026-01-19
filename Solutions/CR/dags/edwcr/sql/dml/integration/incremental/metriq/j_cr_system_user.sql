DECLARE DUP_COUNT INT64;

BEGIN

BEGIN TRANSACTION;

	 -- Truncate Ingest Working Table for Write Off
	 TRUNCATE TABLE {{ params.param_cr_core_dataset_name }}.cr_system_user;

	 -- Populate Ingest Working Table for Write Off
	 INSERT INTO {{ params.param_cr_core_dataset_name }}.cr_system_user
	 (security_id, user_id_code, user_first_name, user_last_name, source_system_code,dw_last_update_date_time)
     SELECT 
     securityid,
     CASE WHEN UserId = '' THEN NULL ELSE UserId END AS user_id_code,
     rtrim(ltrim(FirstName)) AS user_first_name,
     rtrim(ltrim(LastName)) AS user_last_name,
     'M' AS source_system_code,
     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
     FROM {{params.param_cr_stage_dataset_name}}.cr_system_user_stg
;

 
	SET DUP_COUNT =
	(SELECT COUNT(*)
	FROM
		(SELECT Security_Id
		FROM {{ params.param_cr_core_dataset_name }}.cr_system_user
		GROUP BY Security_Id
		HAVING COUNT(*) > 1));

	IF DUP_COUNT <> 0 THEN
		ROLLBACK TRANSACTION;
		RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_cr_core_dataset_name }}.cr_system_user');
	ELSE
		COMMIT TRANSACTION;
	END IF;

END;