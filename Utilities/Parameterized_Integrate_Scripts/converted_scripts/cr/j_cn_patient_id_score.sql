-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/j_cn_patient_id_score.sql
-- Translated from: bteq
-- Translated to: BigQuery

DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MSG STRING DEFAULT '';
-- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.Patient_ID_Input		   		   ##
-- ##  TARGET  DATABASE	   : EDWCR	 						   ##
-- ##  SOURCE		   : edwcr_staging.Scri_Patient_ID_History		   ##
-- ##	                                                                         ##
-- ##  INITIAL RELEASE	   : 								   ##
-- ##  PROJECT            	   : 	 		    				   ##
-- ##  ------------------------------------------------------------------------	   ##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF > $1;
-- SET QUERY_BAND = 'App=EDWCR_ETL; Job=J_CN_Patient_ID_Score;' FOR SESSION;
BEGIN
  SET _ERROR_CODE = 0;
  CREATE TEMPORARY TABLE results
    AS
      SELECT
          res.cancer_patient_id_output_sk,
          max(res_stg.unique_message_id) AS unique_message_id,
          max(res_stg.site_and_associated_model_output_score) AS site_and_associated_model_output_score
        FROM
          `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output AS res
          LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_staging.cdm_synthesys_patientid_results AS res_stg ON upper(rtrim(res.message_control_id_text)) = upper(rtrim(res_stg.unique_message_id))
           AND res.user_action_date_time = CASE
            WHEN upper(rtrim(res_stg.user_action_date_time)) = '' THEN CAST(NULL as DATETIME)
            ELSE CAST(trim(concat(substr(res_stg.user_action_date_time, 1, 10), ' ', substr(res_stg.user_action_date_time, 12, 8))) as DATETIME)
          END
        GROUP BY 1, upper(res_stg.unique_message_id), upper(res_stg.site_and_associated_model_output_score)
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
BEGIN
  SET _ERROR_CODE = 0;
  CREATE TEMPORARY TABLE score
    AS
      SELECT
          dt.cancer_patient_id_output_sk,
          dt.score_sequence_num,
          `hca-hin-dev-cur-ops`.bqutil_fns.cw_td_strtok(nullif(results.site_and_associated_model_output_score, ''), ',', 1) AS site_associated_model_output_site_desc,
          `hca-hin-dev-cur-ops`.bqutil_fns.cw_td_strtok(nullif(results.site_and_associated_model_output_score, ''), ',', 2) AS site_associated_model_output_site_score
        FROM
          results
          CROSS JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_patient_id_score AS dt
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
-- BT;
BEGIN
  SET _ERROR_CODE = 0;
  TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.cancer_patient_id_score;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
BEGIN
  SET _ERROR_CODE = 0;
  MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cancer_patient_id_score AS mt USING (
    SELECT DISTINCT
        res.cancer_patient_id_output_sk,
        sc.score_sequence_num,
        substr(res.unique_message_id, 1, 50) AS message_control_id_text,
        substr(sc.site_associated_model_output_site_desc, 1, 255) AS site_associated_model_output_site_desc,
        ROUND(CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(sc.site_associated_model_output_site_score) as NUMERIC), 10, 'ROUND_HALF_EVEN') AS site_associated_model_output_score_num,
        'H' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        results AS res
        INNER JOIN score AS sc ON res.cancer_patient_id_output_sk = sc.cancer_patient_id_output_sk
      WHERE sc.score_sequence_num IS NOT NULL
  ) AS ms
  ON mt.cancer_patient_id_output_sk = ms.cancer_patient_id_output_sk
   AND mt.score_sequence_num = ms.score_sequence_num
   AND mt.message_control_id_text = ms.message_control_id_text
   AND (upper(coalesce(mt.site_associated_model_output_site_desc, '0')) = upper(coalesce(ms.site_associated_model_output_site_desc, '0'))
   AND upper(coalesce(mt.site_associated_model_output_site_desc, '1')) = upper(coalesce(ms.site_associated_model_output_site_desc, '1')))
   AND (coalesce(mt.site_associated_model_output_score_num, NUMERIC '0') = coalesce(ms.site_associated_model_output_score_num, NUMERIC '0')
   AND coalesce(mt.site_associated_model_output_score_num, NUMERIC '1') = coalesce(ms.site_associated_model_output_score_num, NUMERIC '1'))
   AND mt.source_system_code = ms.source_system_code
   AND mt.dw_last_update_date_time = ms.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN
      INSERT (cancer_patient_id_output_sk, score_sequence_num, message_control_id_text, site_associated_model_output_site_desc, site_associated_model_output_score_num, source_system_code, dw_last_update_date_time)
      VALUES (ms.cancer_patient_id_output_sk, ms.score_sequence_num, ms.message_control_id_text, ms.site_associated_model_output_site_desc, ms.site_associated_model_output_score_num, ms.source_system_code, ms.dw_last_update_date_time)
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
-- -----Added new condition to skip NULL values as confirmed with NJ-----
-- -----Added new condition to skip NULL values as confirmed with NJ-----
-- ET;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','Cancer_Patient_Id_Score');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
BEGIN
  SET _ERROR_CODE = 0;
  TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr_staging.cdm_synthesys_patientid_results;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- EOF
