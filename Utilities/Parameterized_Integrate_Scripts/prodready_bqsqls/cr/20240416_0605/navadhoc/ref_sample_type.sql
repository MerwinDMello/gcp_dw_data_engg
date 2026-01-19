DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/ref_sample_type.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.REF_SAMPLE_TYPE                          ##
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
 --Job=J_CR_REF_SAMPLE_TYPE;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','PATIENT_HEME_DISEASE_ASSESSMENT_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_cr_core_dataset_name }}.ref_sample_type AS mt USING
  (SELECT DISTINCT
     (SELECT coalesce(max(ref_sample_type.sample_type_id), 0) AS maxid
      FROM {{ params.param_cr_base_views_dataset_name }}.ref_sample_type) + row_number() OVER (
                                                                                               ORDER BY upper(trim(ssc.sample_type_name))) AS sample_type_id,
                                                                                              substr(trim(ssc.sample_type_name), 1, 50) AS sample_type_name,
                                                                                              'N' AS source_system_code,
                                                                                              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT DISTINCT trim(patient_heme_disease_assessment_stg.samplesourcetype) AS sample_type_name
      FROM {{ params.param_cr_stage_dataset_name }}.patient_heme_disease_assessment_stg
      WHERE trim(patient_heme_disease_assessment_stg.samplesourcetype) IS NOT NULL ) AS ssc
   LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_sample_type AS rtt ON upper(trim(ssc.sample_type_name)) = upper(trim(rtt.sample_type_name))
   WHERE trim(rtt.sample_type_name) IS NULL ) AS ms ON mt.sample_type_id = ms.sample_type_id
AND (upper(coalesce(mt.sample_type_name, '0')) = upper(coalesce(ms.sample_type_name, '0'))
     AND upper(coalesce(mt.sample_type_name, '1')) = upper(coalesce(ms.sample_type_name, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (sample_type_id,
        sample_type_name,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.sample_type_id, ms.sample_type_name, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT sample_type_id
      FROM {{ params.param_cr_core_dataset_name }}.ref_sample_type
      GROUP BY sample_type_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_cr_core_dataset_name }}.ref_sample_type');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','REF_SAMPLE_TYPE');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF