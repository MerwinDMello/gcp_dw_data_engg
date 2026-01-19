DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cancer_patient_id_abstraction.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.Cancer_Patient_Id_Abstraction                 ##
-- ##  TARGET  DATABASE	   : EDWCR	 											##
-- ##  SOURCE		  		   : EDWCR_Staging.Cancer_Abstraction_Stg 				##
-- ##														  						##
-- ##	                                                                        	##
-- ##  INITIAL RELEASE	   : 														##
-- ##  PROJECT            	   : 	 		    									##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_Cancer_Patient_Id_Abstraction;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_STAGING','Cancer_Abstraction_Stg');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr_staging.cancer_patient_id_abstraction_wrk0;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


INSERT INTO `hca-hin-dev-cur-ops`.edwcr_staging.cancer_patient_id_abstraction_wrk0 (cancer_abstraction_sk, cancer_patient_id_output_sk, message_control_id_text, coid, company_code, abstraction_action_date_time, patient_dw_id, pat_acct_num, abstraction_report_assigned_date_time, abstraction_date_time, abstraction_action_user_3_4_id, abstraction_action_desc, primary_icd_oncology_code, primary_icd_site_desc, primary_icd_site_and_model_score_desc, changed_primary_icd_oncology_code, changed_primary_icd_site_desc, topography_icd_oncology_code, topography_icd_site_desc, laterality_icd_oncology_code, laterality_icd_site_desc, secondary_icd_oncology_code, secondary_icd_site_desc, source_system_code, dw_last_update_date_time)
SELECT NULL AS cancer_abstraction_sk, /* nvl(ROW_NUMBER() OVER ( ORDER BY unique_message_id || ' - ' ||COALESCE(site_and_associated_model_output_score,'' )||COALESCE(second_primary_site,'') ),0)as Cancer_Abstraction_SK ------Added for CES 17570 */ coalesce(o.cancer_patient_id_output_sk, -1) AS cancer_patient_id_output_sk,
                                                                                                                                                                                                                                                        substr(a.unique_message_id, 1, 50) AS message_control_id_text,
                                                                                                                                                                                                                                                        a.coid AS coid,
                                                                                                                                                                                                                                                        'H' AS company_code,
                                                                                                                                                                                                                                                        parse_datetime('%Y-%m-%d %H:%M:%S', substr(replace(a.abstraction_action_date_time, 'T', ' '), 1, 19)) AS abstraction_action_date_time,
                                                                                                                                                                                                                                                        cact.patient_dw_id AS patient_dw_id,
                                                                                                                                                                                                                                                        ROUND(CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(translate(a.patient_account_number, translate(a.patient_account_number, '0123456789', ''), '')) AS NUMERIC), 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
                                                                                                                                                                                                                                                        parse_datetime('%Y-%m-%d %H:%M:%S', substr(replace(a.abstraction_report_assigned_to_user_at, 'T', ' '), 1, 19)) AS abstraction_report_assigned_date_time,
                                                                                                                                                                                                                                                        parse_datetime('%Y-%m-%d %H:%M:%S', substr(replace(a.abstraction_date_time, 'T', ' '), 1, 19)) AS abstraction_date_time,
                                                                                                                                                                                                                                                        a.abstraction_action_user AS abstraction_action_user_3_4_id,
                                                                                                                                                                                                                                                        substr(a.abstraction_action, 1, 20) AS abstraction_action_desc,
                                                                                                                                                                                                                                                        a.submitted_primary_site_icdo3 AS primary_icd_oncology_code,
                                                                                                                                                                                                                                                        substr(a.submitted_primary_site, 1, 255) AS primary_icd_site_desc,
                                                                                                                                                                                                                                                        substr(a.site_and_associated_model_output_score, 1, 255) AS primary_icd_site_and_model_score_desc,
                                                                                                                                                                                                                                                        a.abstraction_changed_primary_site_icdo3 AS changed_primary_icd_oncology_code,
                                                                                                                                                                                                                                                        substr(a.abstraction_changed_primary_site, 1, 255) AS changed_primary_icd_site_desc,
                                                                                                                                                                                                                                                        a.abstraction_topography_icdo3 AS topography_icd_oncology_code,
                                                                                                                                                                                                                                                        substr(a.abstraction_topography, 1, 255) AS topography_icd_site_desc,
                                                                                                                                                                                                                                                        a.abstraction_laterality_code AS laterality_icd_oncology_code,
                                                                                                                                                                                                                                                        substr(a.abstraction_laterality, 1, 255) AS laterality_icd_site_desc,
                                                                                                                                                                                                                                                        a.second_primary_site_icdo3 AS secondary_icd_oncology_code,
                                                                                                                                                                                                                                                        substr(a.second_primary_site, 1, 255) AS secondary_icd_site_desc,
                                                                                                                                                                                                                                                        'H' AS source_system_code,
                                                                                                                                                                                                                                                        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
FROM `hca-hin-dev-cur-ops`.edwcr_staging.cancer_abstraction_stg AS a
LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output AS o ON upper(trim(a.unique_message_id)) = upper(trim(o.message_control_id_text))
LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.clinical_acctkeys AS cact ON ROUND(CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(translate(a.patient_account_number, translate(a.patient_account_number, '0123456789', ''), '')) AS NUMERIC), 0, 'ROUND_HALF_EVEN') = cact.pat_acct_num
AND upper(trim(a.coid)) = upper(trim(cact.coid))
AND upper(rtrim(cact.company_code)) = 'H' QUALIFY /* QUALIFY ROW_NUMBER() OVER(partition by unique_message_id ORDER BY CAST(CAST(OREPLACE(A.abstraction_action_date_time, 'T', ' ') AS VARCHAR(19)) AS TIMESTAMP(0) FORMAT 'YYYY-MM-DDBHH:MI:SS') DESC) = 1;
*/ row_number() OVER (PARTITION BY upper(a.unique_message_id),
                                   upper(a.site_and_associated_model_output_score),
                                   upper(a.second_primary_site)
                      ORDER BY abstraction_action_date_time DESC) = 1;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.COLLECT_STATS_TABLE('EDWCR_STAGING','Cancer_Patient_Id_Abstraction_Wrk0');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr_staging.cancer_patient_id_abstraction_wrk1;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

/*Generate SK for new record*/ IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


INSERT INTO `hca-hin-dev-cur-ops`.edwcr_staging.cancer_patient_id_abstraction_wrk1 (cancer_abstraction_sk, cancer_patient_id_output_sk, message_control_id_text, coid, company_code, abstraction_action_date_time, patient_dw_id, pat_acct_num, abstraction_report_assigned_date_time, abstraction_date_time, abstraction_action_user_3_4_id, abstraction_action_desc, primary_icd_oncology_code, primary_icd_site_desc, primary_icd_site_and_model_score_desc, changed_primary_icd_oncology_code, changed_primary_icd_site_desc, topography_icd_oncology_code, topography_icd_site_desc, laterality_icd_oncology_code, laterality_icd_site_desc, secondary_icd_oncology_code, secondary_icd_site_desc, source_system_code, dw_last_update_date_time)
SELECT -- -nvl(ROW_NUMBER() OVER ( ORDER BY wr.Message_Control_Id_Text ASC) +
 coalesce(row_number() OVER (
                             ORDER BY upper(wr.message_control_id_text), upper(wr.secondary_icd_site_desc), upper(wr.primary_icd_site_and_model_score_desc)) +
            (SELECT coalesce(max(cancer_patient_id_abstraction.cancer_abstraction_sk), 0) AS max_val
             FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_abstraction), 0) AS cancer_abstraction_sk,
 wr.cancer_patient_id_output_sk,
 wr.message_control_id_text,
 wr.coid,
 wr.company_code,
 wr.abstraction_action_date_time,
 wr.patient_dw_id,
 wr.pat_acct_num,
 wr.abstraction_report_assigned_date_time,
 wr.abstraction_date_time,
 wr.abstraction_action_user_3_4_id,
 wr.abstraction_action_desc,
 wr.primary_icd_oncology_code,
 wr.primary_icd_site_desc,
 wr.primary_icd_site_and_model_score_desc,
 wr.changed_primary_icd_oncology_code,
 wr.changed_primary_icd_site_desc,
 wr.topography_icd_oncology_code,
 wr.topography_icd_site_desc,
 wr.laterality_icd_oncology_code,
 wr.laterality_icd_site_desc,
 wr.secondary_icd_oncology_code,
 wr.secondary_icd_site_desc,
 wr.source_system_code,
 wr.dw_last_update_date_time
FROM `hca-hin-dev-cur-ops`.edwcr_staging.cancer_patient_id_abstraction_wrk0 AS wr
LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_abstraction AS cr ON upper(trim(coalesce(wr.message_control_id_text, '##'))) = upper(trim(coalesce(cr.message_control_id_text, '##')))
AND upper(trim(coalesce(wr.secondary_icd_site_desc, '##'))) = upper(trim(coalesce(cr.secondary_icd_site_desc, '##')))
AND upper(trim(coalesce(wr.primary_icd_site_and_model_score_desc, '##'))) = upper(trim(coalesce(cr.primary_icd_site_and_model_score_desc, '##')))
WHERE cr.message_control_id_text IS NULL;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

-- -Added for CES 17570
-- -Added for CES 17570
IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


INSERT INTO `hca-hin-dev-cur-ops`.edwcr_staging.cancer_patient_id_abstraction_wrk1 (cancer_abstraction_sk, cancer_patient_id_output_sk, message_control_id_text, coid, company_code, abstraction_action_date_time, patient_dw_id, pat_acct_num, abstraction_report_assigned_date_time, abstraction_date_time, abstraction_action_user_3_4_id, abstraction_action_desc, primary_icd_oncology_code, primary_icd_site_desc, primary_icd_site_and_model_score_desc, changed_primary_icd_oncology_code, changed_primary_icd_site_desc, topography_icd_oncology_code, topography_icd_site_desc, laterality_icd_oncology_code, laterality_icd_site_desc, secondary_icd_oncology_code, secondary_icd_site_desc, source_system_code, dw_last_update_date_time)
SELECT cr.cancer_abstraction_sk,
       wr.cancer_patient_id_output_sk,
       wr.message_control_id_text,
       wr.coid,
       wr.company_code,
       wr.abstraction_action_date_time,
       wr.patient_dw_id,
       wr.pat_acct_num,
       wr.abstraction_report_assigned_date_time,
       wr.abstraction_date_time,
       wr.abstraction_action_user_3_4_id,
       wr.abstraction_action_desc,
       wr.primary_icd_oncology_code,
       wr.primary_icd_site_desc,
       wr.primary_icd_site_and_model_score_desc,
       wr.changed_primary_icd_oncology_code,
       wr.changed_primary_icd_site_desc,
       wr.topography_icd_oncology_code,
       wr.topography_icd_site_desc,
       wr.laterality_icd_oncology_code,
       wr.laterality_icd_site_desc,
       wr.secondary_icd_oncology_code,
       wr.secondary_icd_site_desc,
       wr.source_system_code,
       wr.dw_last_update_date_time
FROM `hca-hin-dev-cur-ops`.edwcr_staging.cancer_patient_id_abstraction_wrk0 AS wr
LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_abstraction AS cr ON upper(trim(coalesce(wr.message_control_id_text, '##'))) = upper(trim(coalesce(cr.message_control_id_text, '##')))
AND upper(trim(coalesce(wr.secondary_icd_site_desc, '##'))) = upper(trim(coalesce(cr.secondary_icd_site_desc, '##')))
AND upper(trim(coalesce(wr.primary_icd_site_and_model_score_desc, '##'))) = upper(trim(coalesce(cr.primary_icd_site_and_model_score_desc, '##')))
WHERE cr.cancer_abstraction_sk IS NOT NULL;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

-- -Added for CES 17570
-- -Added for CES 17570
IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.COLLECT_STATS_TABLE('EDWCR_STAGING','Cancer_Patient_Id_Abstraction_Wrk1');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr_staging.cancer_patient_id_abstraction_wrk2;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


INSERT INTO `hca-hin-dev-cur-ops`.edwcr_staging.cancer_patient_id_abstraction_wrk2 (cancer_abstraction_sk, cancer_patient_id_output_sk, message_control_id_text, coid, company_code, abstraction_action_date_time, patient_dw_id, pat_acct_num, abstraction_report_assigned_date_time, abstraction_date_time, abstraction_action_user_3_4_id, abstraction_action_desc, primary_icd_oncology_code, primary_icd_site_desc, primary_icd_site_and_model_score_desc, changed_primary_icd_oncology_code, changed_primary_icd_site_desc, topography_icd_oncology_code, topography_icd_site_desc, laterality_icd_oncology_code, laterality_icd_site_desc, secondary_icd_oncology_code, secondary_icd_site_desc, source_system_code, dw_last_update_date_time)
SELECT stg.cancer_abstraction_sk,
       stg.cancer_patient_id_output_sk,
       stg.message_control_id_text,
       stg.coid,
       stg.company_code,
       stg.abstraction_action_date_time,
       stg.patient_dw_id,
       stg.pat_acct_num,
       stg.abstraction_report_assigned_date_time,
       stg.abstraction_date_time,
       stg.abstraction_action_user_3_4_id,
       stg.abstraction_action_desc,
       stg.primary_icd_oncology_code,
       stg.primary_icd_site_desc,
       stg.primary_icd_site_and_model_score_desc,
       stg.changed_primary_icd_oncology_code,
       stg.changed_primary_icd_site_desc,
       stg.topography_icd_oncology_code,
       stg.topography_icd_site_desc,
       stg.laterality_icd_oncology_code,
       stg.laterality_icd_site_desc,
       stg.secondary_icd_oncology_code,
       stg.secondary_icd_site_desc,
       stg.source_system_code,
       stg.dw_last_update_date_time
FROM `hca-hin-dev-cur-ops`.edwcr_staging.cancer_patient_id_abstraction_wrk1 AS stg QUALIFY row_number() OVER (PARTITION BY stg.cancer_abstraction_sk
                                                                                                              ORDER BY stg.abstraction_action_date_time DESC) = 1;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.COLLECT_STATS_TABLE('EDWCR_STAGING','Cancer_Patient_Id_Abstraction_Wrk2');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- BT;
 BEGIN
SET _ERROR_CODE = 0;


UPDATE `hca-hin-dev-cur-ops`.edwcr.cancer_patient_id_abstraction
SET cancer_patient_id_output_sk = src.cancer_patient_id_output_sk,
    message_control_id_text = src.message_control_id_text,
    coid = src.coid,
    company_code = src.company_code,
    abstraction_action_date_time = src.abstraction_action_date_time,
    patient_dw_id = src.patient_dw_id,
    pat_acct_num = src.pat_acct_num,
    abstraction_report_assigned_date_time = src.abstraction_report_assigned_date_time,
    abstraction_date_time = src.abstraction_date_time,
    abstraction_action_user_3_4_id = src.abstraction_action_user_3_4_id,
    abstraction_action_desc = src.abstraction_action_desc,
    primary_icd_oncology_code = src.primary_icd_oncology_code,
    primary_icd_site_desc = src.primary_icd_site_desc,
    primary_icd_site_and_model_score_desc = src.primary_icd_site_and_model_score_desc,
    changed_primary_icd_oncology_code = src.changed_primary_icd_oncology_code,
    changed_primary_icd_site_desc = src.changed_primary_icd_site_desc,
    topography_icd_oncology_code = src.topography_icd_oncology_code,
    topography_icd_site_desc = src.topography_icd_site_desc,
    laterality_icd_oncology_code = src.laterality_icd_oncology_code,
    laterality_icd_site_desc = src.laterality_icd_site_desc,
    secondary_icd_oncology_code = src.secondary_icd_oncology_code,
    secondary_icd_site_desc = src.secondary_icd_site_desc,
    source_system_code = src.source_system_code,
    dw_last_update_date_time = src.dw_last_update_date_time
FROM `hca-hin-dev-cur-ops`.edwcr_staging.cancer_patient_id_abstraction_wrk2 AS src
WHERE cancer_patient_id_abstraction.cancer_abstraction_sk = src.cancer_abstraction_sk;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cancer_patient_id_abstraction AS mt USING
  (SELECT DISTINCT stg.cancer_abstraction_sk,
                   stg.cancer_patient_id_output_sk,
                   stg.message_control_id_text,
                   stg.coid AS coid,
                   stg.company_code AS company_code,
                   stg.abstraction_action_date_time,
                   stg.patient_dw_id,
                   stg.pat_acct_num,
                   stg.abstraction_report_assigned_date_time,
                   stg.abstraction_date_time,
                   stg.abstraction_action_user_3_4_id AS abstraction_action_user_3_4_id,
                   stg.abstraction_action_desc,
                   stg.primary_icd_oncology_code AS primary_icd_oncology_code,
                   stg.primary_icd_site_desc,
                   stg.primary_icd_site_and_model_score_desc,
                   stg.changed_primary_icd_oncology_code AS changed_primary_icd_oncology_code,
                   stg.changed_primary_icd_site_desc,
                   stg.topography_icd_oncology_code AS topography_icd_oncology_code,
                   stg.topography_icd_site_desc,
                   stg.laterality_icd_oncology_code AS laterality_icd_oncology_code,
                   stg.laterality_icd_site_desc,
                   stg.secondary_icd_oncology_code AS secondary_icd_oncology_code,
                   stg.secondary_icd_site_desc,
                   stg.source_system_code AS source_system_code,
                   stg.dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.cancer_patient_id_abstraction_wrk2 AS stg
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_abstraction AS tgt ON stg.cancer_abstraction_sk = tgt.cancer_abstraction_sk
   WHERE tgt.cancer_abstraction_sk IS NULL ) AS ms ON mt.cancer_abstraction_sk = ms.cancer_abstraction_sk
AND mt.cancer_patient_id_output_sk = ms.cancer_patient_id_output_sk
AND (upper(coalesce(mt.message_control_id_text, '0')) = upper(coalesce(ms.message_control_id_text, '0'))
     AND upper(coalesce(mt.message_control_id_text, '1')) = upper(coalesce(ms.message_control_id_text, '1')))
AND (upper(coalesce(mt.coid, '0')) = upper(coalesce(ms.coid, '0'))
     AND upper(coalesce(mt.coid, '1')) = upper(coalesce(ms.coid, '1')))
AND (upper(coalesce(mt.company_code, '0')) = upper(coalesce(ms.company_code, '0'))
     AND upper(coalesce(mt.company_code, '1')) = upper(coalesce(ms.company_code, '1')))
AND (coalesce(mt.abstraction_action_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.abstraction_action_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.abstraction_action_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.abstraction_action_date_time, DATETIME '1970-01-01 00:00:01'))
AND (coalesce(mt.patient_dw_id, NUMERIC '0') = coalesce(ms.patient_dw_id, NUMERIC '0')
     AND coalesce(mt.patient_dw_id, NUMERIC '1') = coalesce(ms.patient_dw_id, NUMERIC '1'))
AND (coalesce(mt.pat_acct_num, NUMERIC '0') = coalesce(ms.pat_acct_num, NUMERIC '0')
     AND coalesce(mt.pat_acct_num, NUMERIC '1') = coalesce(ms.pat_acct_num, NUMERIC '1'))
AND (coalesce(mt.abstraction_report_assigned_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.abstraction_report_assigned_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.abstraction_report_assigned_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.abstraction_report_assigned_date_time, DATETIME '1970-01-01 00:00:01'))
AND (coalesce(mt.abstraction_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.abstraction_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.abstraction_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.abstraction_date_time, DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.abstraction_action_user_3_4_id, '0')) = upper(coalesce(ms.abstraction_action_user_3_4_id, '0'))
     AND upper(coalesce(mt.abstraction_action_user_3_4_id, '1')) = upper(coalesce(ms.abstraction_action_user_3_4_id, '1')))
AND (upper(coalesce(mt.abstraction_action_desc, '0')) = upper(coalesce(ms.abstraction_action_desc, '0'))
     AND upper(coalesce(mt.abstraction_action_desc, '1')) = upper(coalesce(ms.abstraction_action_desc, '1')))
AND (upper(coalesce(mt.primary_icd_oncology_code, '0')) = upper(coalesce(ms.primary_icd_oncology_code, '0'))
     AND upper(coalesce(mt.primary_icd_oncology_code, '1')) = upper(coalesce(ms.primary_icd_oncology_code, '1')))
AND (upper(coalesce(mt.primary_icd_site_desc, '0')) = upper(coalesce(ms.primary_icd_site_desc, '0'))
     AND upper(coalesce(mt.primary_icd_site_desc, '1')) = upper(coalesce(ms.primary_icd_site_desc, '1')))
AND (upper(coalesce(mt.primary_icd_site_and_model_score_desc, '0')) = upper(coalesce(ms.primary_icd_site_and_model_score_desc, '0'))
     AND upper(coalesce(mt.primary_icd_site_and_model_score_desc, '1')) = upper(coalesce(ms.primary_icd_site_and_model_score_desc, '1')))
AND (upper(coalesce(mt.changed_primary_icd_oncology_code, '0')) = upper(coalesce(ms.changed_primary_icd_oncology_code, '0'))
     AND upper(coalesce(mt.changed_primary_icd_oncology_code, '1')) = upper(coalesce(ms.changed_primary_icd_oncology_code, '1')))
AND (upper(coalesce(mt.changed_primary_icd_site_desc, '0')) = upper(coalesce(ms.changed_primary_icd_site_desc, '0'))
     AND upper(coalesce(mt.changed_primary_icd_site_desc, '1')) = upper(coalesce(ms.changed_primary_icd_site_desc, '1')))
AND (upper(coalesce(mt.topography_icd_oncology_code, '0')) = upper(coalesce(ms.topography_icd_oncology_code, '0'))
     AND upper(coalesce(mt.topography_icd_oncology_code, '1')) = upper(coalesce(ms.topography_icd_oncology_code, '1')))
AND (upper(coalesce(mt.topography_icd_site_desc, '0')) = upper(coalesce(ms.topography_icd_site_desc, '0'))
     AND upper(coalesce(mt.topography_icd_site_desc, '1')) = upper(coalesce(ms.topography_icd_site_desc, '1')))
AND (upper(coalesce(mt.laterality_icd_oncology_code, '0')) = upper(coalesce(ms.laterality_icd_oncology_code, '0'))
     AND upper(coalesce(mt.laterality_icd_oncology_code, '1')) = upper(coalesce(ms.laterality_icd_oncology_code, '1')))
AND (upper(coalesce(mt.laterality_icd_site_desc, '0')) = upper(coalesce(ms.laterality_icd_site_desc, '0'))
     AND upper(coalesce(mt.laterality_icd_site_desc, '1')) = upper(coalesce(ms.laterality_icd_site_desc, '1')))
AND (upper(coalesce(mt.secondary_icd_oncology_code, '0')) = upper(coalesce(ms.secondary_icd_oncology_code, '0'))
     AND upper(coalesce(mt.secondary_icd_oncology_code, '1')) = upper(coalesce(ms.secondary_icd_oncology_code, '1')))
AND (upper(coalesce(mt.secondary_icd_site_desc, '0')) = upper(coalesce(ms.secondary_icd_site_desc, '0'))
     AND upper(coalesce(mt.secondary_icd_site_desc, '1')) = upper(coalesce(ms.secondary_icd_site_desc, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (cancer_abstraction_sk,
        cancer_patient_id_output_sk,
        message_control_id_text,
        coid,
        company_code,
        abstraction_action_date_time,
        patient_dw_id,
        pat_acct_num,
        abstraction_report_assigned_date_time,
        abstraction_date_time,
        abstraction_action_user_3_4_id,
        abstraction_action_desc,
        primary_icd_oncology_code,
        primary_icd_site_desc,
        primary_icd_site_and_model_score_desc,
        changed_primary_icd_oncology_code,
        changed_primary_icd_site_desc,
        topography_icd_oncology_code,
        topography_icd_site_desc,
        laterality_icd_oncology_code,
        laterality_icd_site_desc,
        secondary_icd_oncology_code,
        secondary_icd_site_desc,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.cancer_abstraction_sk, ms.cancer_patient_id_output_sk, ms.message_control_id_text, ms.coid, ms.company_code, ms.abstraction_action_date_time, ms.patient_dw_id, ms.pat_acct_num, ms.abstraction_report_assigned_date_time, ms.abstraction_date_time, ms.abstraction_action_user_3_4_id, ms.abstraction_action_desc, ms.primary_icd_oncology_code, ms.primary_icd_site_desc, ms.primary_icd_site_and_model_score_desc, ms.changed_primary_icd_oncology_code, ms.changed_primary_icd_site_desc, ms.topography_icd_oncology_code, ms.topography_icd_site_desc, ms.laterality_icd_oncology_code, ms.laterality_icd_site_desc, ms.secondary_icd_oncology_code, ms.secondary_icd_site_desc, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT cancer_abstraction_sk
      FROM `hca-hin-dev-cur-ops`.edwcr.cancer_patient_id_abstraction
      GROUP BY cancer_abstraction_sk
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.cancer_patient_id_abstraction');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- ET;
 -- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','Cancer_Patient_Id_Abstraction');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF;;