	BEGIN
	  DECLARE dup_count INT64;
	  DECLARE current_dt DATETIME;
	  SET current_dt = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

	  BEGIN TRANSACTION;

	  UPDATE {{ params.param_hr_core_dataset_name }}.recruitment_user AS tgt SET valid_to_date = current_dt - INTERVAL 1 SECOND, dw_last_update_date_time = stg.dw_last_update_date_time FROM {{ params.param_hr_stage_dataset_name }}.taleo_user_wrk AS stg WHERE tgt.recruitment_user_sid = stg.recruitment_user_sid
	   AND (trim(CAST(coalesce(tgt.recruitment_user_num, -999) as STRING)) <> trim(CAST(coalesce(stg.recruitment_user_num, -999) as STRING))
	   OR trim(CAST(coalesce(tgt.employee_num, -999) as STRING)) <> trim(CAST(coalesce(stg.employee_num, -999) as STRING))
	   OR upper(trim(coalesce(tgt.employee_34_login_code, ''))) <> upper(trim(coalesce(stg.employee_34_login_code, '')))
	   OR upper(trim(coalesce(tgt.first_name, ''))) <> upper(trim(coalesce(stg.first_name, '')))
	   OR upper(trim(coalesce(tgt.last_name, ''))) <> upper(trim(coalesce(stg.last_name, '')))
	   OR trim(tgt.source_system_code) <> trim(stg.source_system_code))
	   AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59");


	  INSERT INTO {{ params.param_hr_core_dataset_name }}.recruitment_user (recruitment_user_sid, valid_from_date, recruitment_user_num, valid_to_date, employee_num, employee_34_login_code, first_name, last_name, source_system_code, dw_last_update_date_time)
		SELECT
			stg.recruitment_user_sid,
			current_dt,
			stg.recruitment_user_num,
			DATETIME("9999-12-31 23:59:59"),
			stg.employee_num,
			stg.employee_34_login_code,
			stg.first_name,
			stg.last_name,
			stg.source_system_code,
			stg.dw_last_update_date_time
		  FROM
			{{ params.param_hr_stage_dataset_name }}.taleo_user_wrk AS stg
			LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_user AS tgt ON stg.recruitment_user_sid = tgt.recruitment_user_sid
			 AND trim(CAST(coalesce(tgt.recruitment_user_num, -999) as STRING)) = trim(CAST(coalesce(stg.recruitment_user_num, -999) as STRING))
			 AND trim(CAST(coalesce(tgt.employee_num, -999) as STRING)) = trim(CAST(coalesce(stg.employee_num, -999) as STRING))
			 AND upper(trim(coalesce(tgt.employee_34_login_code, ''))) = upper(trim(coalesce(stg.employee_34_login_code, '')))
			 AND upper(trim(coalesce(tgt.first_name, ''))) = upper(trim(coalesce(stg.first_name, '')))
			 AND upper(trim(coalesce(tgt.last_name, ''))) = upper(trim(coalesce(stg.last_name, '')))
			 AND trim(tgt.source_system_code) = trim(stg.source_system_code)
			 AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
		  WHERE tgt.recruitment_user_sid IS NULL
	  ;


		/* Test Unique Index constraint set in Terdata */
		SET dup_count = ( 
			select count(*)
			from (
			select
				recruitment_user_sid ,valid_from_date
			from {{ params.param_hr_core_dataset_name }}.recruitment_user
			group by recruitment_user_sid ,valid_from_date
			having count(*) > 1
			)
		);
		IF dup_count <> 0 THEN
		  ROLLBACK TRANSACTION;
		  RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.recruitment_user');
		ELSE
		  COMMIT TRANSACTION;
		END IF;
	END;