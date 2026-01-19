DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/ref_disease_assess_source.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.REF_DISEASE_ASSESS_SOURCE                          ##
-- ##  TARGET  DATABASE	   : EDWCR	 					##
-- ##  SOURCE		   :       EDWCR_staging.PATIENT_HEME_DISEASE_ASSESSMENT_STG     		##
-- ##	                                                                        ##
-- ##  INITIAL RELEASE	   : 							##
-- ##  PROJECT            	   : 	 		    				##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_REF_DISEASE_ASSESS_SOURCE;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','PATIENT_HEME_DISEASE_ASSESSMENT_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_cr_core_dataset_name }}.ref_disease_assess_source AS mt USING
  (SELECT DISTINCT
     (SELECT coalesce(max(ref_disease_assess_source.disease_assess_source_id), 0) AS maxid
      FROM {{ params.param_cr_base_views_dataset_name }}.ref_disease_assess_source) + row_number() OVER (
                                                                                                         ORDER BY upper(trim(ssc.disease_assess_source_name))) AS disease_assess_source_id,
                                                                                                        substr(trim(ssc.disease_assess_source_name), 1, 50) AS disease_assess_source_name,
                                                                                                        'N' AS source_system_code,
                                                                                                        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT DISTINCT trim(patient_heme_disease_assessment_stg.source) AS disease_assess_source_name
      FROM {{ params.param_cr_stage_dataset_name }}.patient_heme_disease_assessment_stg
      WHERE trim(patient_heme_disease_assessment_stg.source) IS NOT NULL ) AS ssc
   LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_disease_assess_source AS rtt ON upper(trim(ssc.disease_assess_source_name)) = upper(trim(rtt.disease_assess_source_name))
   WHERE trim(rtt.disease_assess_source_name) IS NULL ) AS ms ON mt.disease_assess_source_id = ms.disease_assess_source_id
AND (upper(coalesce(mt.disease_assess_source_name, '0')) = upper(coalesce(ms.disease_assess_source_name, '0'))
     AND upper(coalesce(mt.disease_assess_source_name, '1')) = upper(coalesce(ms.disease_assess_source_name, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (disease_assess_source_id,
        disease_assess_source_name,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.disease_assess_source_id, ms.disease_assess_source_name, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT disease_assess_source_id
      FROM {{ params.param_cr_core_dataset_name }}.ref_disease_assess_source
      GROUP BY disease_assess_source_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_cr_core_dataset_name }}.ref_disease_assess_source');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','REF_DISEASE_ASSESS_SOURCE');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF