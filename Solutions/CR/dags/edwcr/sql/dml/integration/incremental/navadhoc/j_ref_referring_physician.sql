DECLARE DUP_COUNT INT64;

BEGIN

BEGIN TRANSACTION;

	 -- Truncate Ingest Working Table for Write Off
	 TRUNCATE TABLE {{ params.param_cr_core_dataset_name }}.ref_referring_physician;

	 -- Populate Ingest Working Table for Write Off
	 INSERT INTO {{ params.param_cr_core_dataset_name }}.ref_referring_physician
	 (referring_physician_id, referring_physician_name, navigation_referred_ind, biopsy_referred_ind, surgery_referred_ind,
	 source_system_code, dw_last_update_date_time)
	 SELECT
		ref_referring_physician.referring_physician_id,
		trim(ref_referring_physician.referring_physician_name) AS referring_physician_name,
		CASE
		  WHEN CAST({{ params.param_cr_bqutil_fns_dataset_name }}.cw_td_normalize_number(ref_referring_physician.navigation_referred_ind) as FLOAT64) = 1 THEN 'Y'
		  ELSE 'N'
		END AS navigation_referred_ind,
		CASE
		  WHEN CAST({{ params.param_cr_bqutil_fns_dataset_name }}.cw_td_normalize_number(ref_referring_physician.biopsy_referred_ind) as FLOAT64) = 1 THEN 'Y'
		  ELSE 'N'
		END AS biopsy_referred_ind,
		CASE
		  WHEN CAST({{ params.param_cr_bqutil_fns_dataset_name }}.cw_td_normalize_number(ref_referring_physician.surgery_referred_ind) as FLOAT64) = 1 THEN 'Y'
		  ELSE 'N'
		END AS surgery_referred_ind,
		'N' AS source_system_code,
		datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
	  FROM
		{{ params.param_cr_stage_dataset_name }}.ref_referring_physician
;
 
	SET DUP_COUNT =
	(SELECT COUNT(*)
	FROM
		(SELECT referring_physician_id
		FROM {{ params.param_cr_core_dataset_name }}.ref_referring_physician
		GROUP BY referring_physician_id
		HAVING COUNT(*) > 1));

	IF DUP_COUNT <> 0 THEN
		ROLLBACK TRANSACTION;
		RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_cr_core_dataset_name }}.ref_referring_physician');
	ELSE
		COMMIT TRANSACTION;
	END IF;

END;