DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_physician_role.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #####################################################################################
-- #  TARGET TABLE		: EDWCR.CN_PHYSICIAN_ROLE	              	#
-- #  TARGET  DATABASE	   	: EDWCR	 						#
-- #  SOURCE		   	: EDWCR_STAGING.CN_PHYSICIAN_ROLE_STG#
-- #	                                                                        	#
-- #  INITIAL RELEASE	   	: 							#
-- #  PROJECT             	: 	 		    					#
-- #  ------------------------------------------------------------------------		#
-- #                                                                              		#
-- #####################################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CN_PHYSICIAN_ROLE;;
 --' FOR SESSION;;
 /* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','CN_PHYSICIAN_ROLE_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Insert the new records into the Core Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_cr_core_dataset_name }}.cn_physician_role AS mt USING
  (SELECT DISTINCT ab.physician_id,
                   ab.physician_role_code AS physician_role_code,
                   'N' AS source_system_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT cn_physician_role_stg.physician_id,
             cn_physician_role_stg.physician_role_code
      FROM {{ params.param_cr_stage_dataset_name }}.cn_physician_role_stg
      UNION DISTINCT SELECT cn_physician_detail.physician_id,
                            'GYN' AS physician_role_code
      FROM {{ params.param_cr_core_dataset_name }}.cn_physician_detail
      INNER JOIN {{ params.param_cr_core_dataset_name }}.cn_patient ON cn_physician_detail.physician_id = cn_patient.gynecologist_physician_id
      UNION DISTINCT SELECT cn_physician_detail.physician_id,
                            'PCP' AS physician_role_code
      FROM {{ params.param_cr_core_dataset_name }}.cn_physician_detail
      INNER JOIN {{ params.param_cr_core_dataset_name }}.cn_patient ON cn_physician_detail.physician_id = cn_patient.primary_care_physician_id
      UNION DISTINCT SELECT cn_physician_detail.physician_id,
                            'ETP' AS physician_role_code
      FROM {{ params.param_cr_core_dataset_name }}.cn_physician_detail
      INNER JOIN {{ params.param_cr_core_dataset_name }}.cn_patient_tumor ON cn_physician_detail.physician_id = cn_patient_tumor.treatment_end_physician_id
      UNION DISTINCT SELECT cn_physician_detail.physician_id,
                            'HEM' AS physician_role_code
      FROM {{ params.param_cr_core_dataset_name }}.cn_physician_detail
      INNER JOIN {{ params.param_cr_core_dataset_name }}.cn_patient_heme ON cn_physician_detail.physician_id = cn_patient_heme.hematologist_physician_id) AS ab
   WHERE (ab.physician_id,
          upper(ab.physician_role_code)) NOT IN
       (SELECT AS STRUCT cn_physician_role.physician_id,
                         upper(cn_physician_role.physician_role_code) AS physician_role_code
        FROM {{ params.param_cr_core_dataset_name }}.cn_physician_role) ) AS ms ON mt.physician_id = ms.physician_id
AND mt.physician_role_code = ms.physician_role_code
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (physician_id,
        physician_role_code,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.physician_id, ms.physician_role_code, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT physician_id,
             physician_role_code
      FROM {{ params.param_cr_core_dataset_name }}.cn_physician_role
      GROUP BY physician_id,
               physician_role_code
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_cr_core_dataset_name }}.cn_physician_role');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','CN_PHYSICIAN_ROLE');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF