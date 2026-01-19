DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cancer_patient_id_abstraction_detail.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.Cancer_Patient_Id_Abstraction_Detail                        	##
-- ##  TARGET  DATABASE	   : EDWCR	 											##
-- ##  SOURCE		  		   : EDWCR_Staging.Cancer_Abstraction_Values_Stg 						##
-- ##														  						##
-- ##	                                                                        	##
-- ##  INITIAL RELEASE	   : 														##
-- ##  PROJECT            	   : 	 		    									##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_Cancer_Patient_Id_Abstraction_Detail;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_STAGING','Cancer_Abstraction_Values_Stg');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE {{ params.param_cr_stage_dataset_name }}.cancer_patient_id_abstraction_detail_wrk0;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


INSERT INTO {{ params.param_cr_stage_dataset_name }}.cancer_patient_id_abstraction_detail_wrk0 (cancer_abstraction_sk, abstraction_measure_sk, cancer_patient_id_output_sk, message_control_id_text, coid, company_code, patient_dw_id, pat_acct_num, predicted_value_text, submitted_value_text, suggested_value_text, source_system_code, dw_last_update_date_time)
SELECT abstrct.cancer_abstraction_sk AS cancer_abstraction_sk,
       measure.abstraction_measure_sk AS abstraction_measure_sk,
       abstrct.cancer_patient_id_output_sk AS cancer_patient_id_output_sk,
       substr(stg.unique_message_id, 1, 50) AS message_control_id_text,
       abstrct.coid AS coid,
       abstrct.company_code AS company_code,
       abstrct.patient_dw_id AS patient_dw_id,
       abstrct.pat_acct_num AS pat_acct_num,
       substr(stg.value_predicted, 1, 255) AS predicted_value_text,
       substr(stg.value_submitted, 1, 255) AS submitted_value_text,
       substr(stg.value_suggested, 1, 255) AS suggested_value_text,
       'H' AS source_system_code,
       datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
FROM {{ params.param_cr_stage_dataset_name }}.cancer_abstraction_values_stg AS stg
INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_abstraction AS abstrct ON upper(trim(stg.unique_message_id)) = upper(trim(abstrct.message_control_id_text))
AND upper(trim(coalesce(stg.second_primary_site, '##'))) = upper(trim(coalesce(abstrct.secondary_icd_site_desc, '##')))
AND upper(trim(coalesce(stg.site_and_associated_model_output_score, '##'))) = upper(trim(coalesce(abstrct.primary_icd_site_and_model_score_desc, '##')))
INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_abstraction_measure AS measure ON upper(trim(stg.abstraction_field)) = upper(trim(measure.abstraction_measure_name)) QUALIFY row_number() OVER (PARTITION BY cancer_abstraction_sk,
                                                                                                                                                                                                                          abstraction_measure_sk
                                                                                                                                                                                                             ORDER BY upper(stg.unique_message_id) DESC) = 1;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.COLLECT_STATS_TABLE('EDWCR_STAGING','Cancer_Patient_Id_Abstraction_Detail_Wrk0');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- BT;
 BEGIN
SET _ERROR_CODE = 0;


UPDATE {{ params.param_cr_core_dataset_name }}.cancer_patient_id_abstraction_detail
SET cancer_patient_id_output_sk = src.cancer_patient_id_output_sk,
    message_control_id_text = src.message_control_id_text,
    coid = src.coid,
    company_code = src.company_code,
    patient_dw_id = src.patient_dw_id,
    pat_acct_num = src.pat_acct_num,
    predicted_value_text = src.predicted_value_text,
    submitted_value_text = src.submitted_value_text,
    suggested_value_text = src.suggested_value_text,
    source_system_code = src.source_system_code,
    dw_last_update_date_time = src.dw_last_update_date_time
FROM {{ params.param_cr_stage_dataset_name }}.cancer_patient_id_abstraction_detail_wrk0 AS src
WHERE cancer_patient_id_abstraction_detail.cancer_abstraction_sk = src.cancer_abstraction_sk
  AND cancer_patient_id_abstraction_detail.abstraction_measure_sk = src.abstraction_measure_sk;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_cr_core_dataset_name }}.cancer_patient_id_abstraction_detail AS mt USING
  (SELECT DISTINCT stg.cancer_abstraction_sk,
                   stg.abstraction_measure_sk,
                   stg.cancer_patient_id_output_sk,
                   stg.message_control_id_text,
                   stg.coid AS coid,
                   stg.company_code AS company_code,
                   stg.patient_dw_id,
                   stg.pat_acct_num,
                   stg.predicted_value_text,
                   stg.submitted_value_text,
                   stg.suggested_value_text,
                   stg.source_system_code AS source_system_code,
                   stg.dw_last_update_date_time
   FROM {{ params.param_cr_stage_dataset_name }}.cancer_patient_id_abstraction_detail_wrk0 AS stg
   LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_abstraction_detail AS tgt ON stg.cancer_abstraction_sk = tgt.cancer_abstraction_sk
   AND stg.abstraction_measure_sk = tgt.abstraction_measure_sk
   WHERE tgt.cancer_abstraction_sk IS NULL ) AS ms ON mt.cancer_abstraction_sk = ms.cancer_abstraction_sk
AND mt.abstraction_measure_sk = ms.abstraction_measure_sk
AND mt.cancer_patient_id_output_sk = ms.cancer_patient_id_output_sk
AND (upper(coalesce(mt.message_control_id_text, '0')) = upper(coalesce(ms.message_control_id_text, '0'))
     AND upper(coalesce(mt.message_control_id_text, '1')) = upper(coalesce(ms.message_control_id_text, '1')))
AND (upper(coalesce(mt.coid, '0')) = upper(coalesce(ms.coid, '0'))
     AND upper(coalesce(mt.coid, '1')) = upper(coalesce(ms.coid, '1')))
AND (upper(coalesce(mt.company_code, '0')) = upper(coalesce(ms.company_code, '0'))
     AND upper(coalesce(mt.company_code, '1')) = upper(coalesce(ms.company_code, '1')))
AND (coalesce(mt.patient_dw_id, NUMERIC '0') = coalesce(ms.patient_dw_id, NUMERIC '0')
     AND coalesce(mt.patient_dw_id, NUMERIC '1') = coalesce(ms.patient_dw_id, NUMERIC '1'))
AND (coalesce(mt.pat_acct_num, NUMERIC '0') = coalesce(ms.pat_acct_num, NUMERIC '0')
     AND coalesce(mt.pat_acct_num, NUMERIC '1') = coalesce(ms.pat_acct_num, NUMERIC '1'))
AND (upper(coalesce(mt.predicted_value_text, '0')) = upper(coalesce(ms.predicted_value_text, '0'))
     AND upper(coalesce(mt.predicted_value_text, '1')) = upper(coalesce(ms.predicted_value_text, '1')))
AND (upper(coalesce(mt.submitted_value_text, '0')) = upper(coalesce(ms.submitted_value_text, '0'))
     AND upper(coalesce(mt.submitted_value_text, '1')) = upper(coalesce(ms.submitted_value_text, '1')))
AND (upper(coalesce(mt.suggested_value_text, '0')) = upper(coalesce(ms.suggested_value_text, '0'))
     AND upper(coalesce(mt.suggested_value_text, '1')) = upper(coalesce(ms.suggested_value_text, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (cancer_abstraction_sk,
        abstraction_measure_sk,
        cancer_patient_id_output_sk,
        message_control_id_text,
        coid,
        company_code,
        patient_dw_id,
        pat_acct_num,
        predicted_value_text,
        submitted_value_text,
        suggested_value_text,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.cancer_abstraction_sk, ms.abstraction_measure_sk, ms.cancer_patient_id_output_sk, ms.message_control_id_text, ms.coid, ms.company_code, ms.patient_dw_id, ms.pat_acct_num, ms.predicted_value_text, ms.submitted_value_text, ms.suggested_value_text, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT cancer_abstraction_sk,
             abstraction_measure_sk
      FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_id_abstraction_detail
      GROUP BY cancer_abstraction_sk,
               abstraction_measure_sk
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_cr_core_dataset_name }}.cancer_patient_id_abstraction_detail');

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
 -- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','Cancer_Patient_Id_Abstraction_Detail');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF