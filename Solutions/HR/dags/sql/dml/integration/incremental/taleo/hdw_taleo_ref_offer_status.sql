	BEGIN
	DECLARE DUP_COUNT INT64;

	BEGIN TRANSACTION;
	  MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_offer_status AS tgt USING (
		SELECT DISTINCT
			CAST(stg.number AS INT64) AS offer_status_id,
			trim(description) AS offer_status_desc,
			'T' AS source_system_code,
			timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
		  FROM
			{{ params.param_hr_stage_dataset_name }}.taleo_offerstatus AS stg
		UNION ALL
		SELECT
			CAST(CASE
			  WHEN tgt_0.offer_status_id IS NOT NULL THEN tgt_0.offer_status_id
			  ELSE (
				SELECT
					coalesce(max(ref_offer_status.offer_status_id), CAST(1000 as INT64))
				  FROM
					{{ params.param_hr_base_views_dataset_name }}.ref_offer_status
				  WHERE upper(ref_offer_status.source_system_code) = 'B'
			  ) + CAST(row_number() OVER (ORDER BY tgt_0.offer_status_id, stgg.offer_status_desc) as BIGNUMERIC)
			END as INT64) AS offer_status_id,
			stgg.offer_status_desc,
			stgg.source_system_code,
			stgg.dw_last_update_date_time
		  FROM
			(
			  SELECT DISTINCT
				  trim(offerstatus_state) AS offer_status_desc,
				  'B' AS source_system_code,
				  timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
				FROM
				 {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg
				WHERE coalesce(trim(offerstatus_state), '') <> ''
			) AS stgg
			LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_offer_status AS tgt_0 ON trim(tgt_0.offer_status_desc) = stgg.offer_status_desc
			 AND upper(tgt_0.source_system_code) = 'B'
	  ) AS src
	  ON tgt.offer_status_id = src.offer_status_id
	   AND upper(tgt.source_system_code) = upper(src.source_system_code)
		 WHEN MATCHED THEN UPDATE SET offer_status_desc = src.offer_status_desc, dw_last_update_date_time = src.dw_last_update_date_time
		 WHEN NOT MATCHED BY TARGET THEN INSERT (offer_status_id, offer_status_desc, source_system_code, dw_last_update_date_time) VALUES (src.offer_status_id, src.offer_status_desc, src.source_system_code, src.dw_last_update_date_time)
	  ;
	  
	  SET DUP_COUNT = (
		SELECT COUNT(*) FROM(
		  SELECT offer_status_id
		  FROM {{ params.param_hr_core_dataset_name }}.ref_offer_status
		  GROUP BY offer_status_id
		  HAVING COUNT(*) > 1
		)
	  );

	  IF DUP_COUNT <> 0 THEN
	  ROLLBACK TRANSACTION;
	  RAISE USING MESSAGE = CONCAT(' Duplicates not allowed in table: ref_offer_status ');
	  ELSE 
	  COMMIT TRANSACTION;
	  END IF;
	END;