DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/ref_diagnosis_detail.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.Ref_Diagnosis_Detail                         ##
-- ##  TARGET  DATABASE	   : EDWCR	 					##
-- ##  SOURCE		   :"EDWCR_staging.Ref_Diagnosis_Detail_Stg		##
-- ##	                                                                        ##
-- ##  INITIAL RELEASE	   : 							##
-- ##  PROJECT            	   : 	 		    				##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_Ref_Diagnosis_Detail;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','Ref_Diagnosis_Detail_Stg');
 BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_cr_core_dataset_name }}.ref_diagnosis_detail AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY upper(trim(type_stg.diagnosis_detail_desc)),
                                               upper(trim(type_stg.diagnosis_indicator_text))) +
     (SELECT coalesce(max(ref_diagnosis_detail.diagnosis_detail_id), 0) AS id1
      FROM {{ params.param_cr_core_dataset_name }}.ref_diagnosis_detail) AS diagnosis_detail_id,
                                     substr(trim(type_stg.diagnosis_detail_desc), 1, 255) AS diagnosis_detail_desc,
                                     substr(trim(type_stg.diagnosis_indicator_text), 1, 20) AS diagnosis_indicator_text,
                                     type_stg.source_system_code AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT trim(ref_diagnosis_detail_stg.diagnosis_detail_desc) AS diagnosis_detail_desc,
             trim(ref_diagnosis_detail_stg.diagnosis_indicator_text) AS diagnosis_indicator_text,
             ref_diagnosis_detail_stg.source_system_code AS source_system_code
      FROM {{ params.param_cr_stage_dataset_name }}.ref_diagnosis_detail_stg
      WHERE (upper(trim(ref_diagnosis_detail_stg.diagnosis_detail_desc)),
             upper(trim(ref_diagnosis_detail_stg.diagnosis_indicator_text))) NOT IN
          (SELECT AS STRUCT upper(trim(ref_diagnosis_detail.diagnosis_detail_desc)),
                            upper(trim(ref_diagnosis_detail.diagnosis_indicator_text))
           FROM {{ params.param_cr_core_dataset_name }}.ref_diagnosis_detail) ) AS type_stg) AS ms ON mt.diagnosis_detail_id = ms.diagnosis_detail_id
AND (upper(coalesce(mt.diagnosis_detail_desc, '0')) = upper(coalesce(ms.diagnosis_detail_desc, '0'))
     AND upper(coalesce(mt.diagnosis_detail_desc, '1')) = upper(coalesce(ms.diagnosis_detail_desc, '1')))
AND (upper(coalesce(mt.diagnosis_indicator_text, '0')) = upper(coalesce(ms.diagnosis_indicator_text, '0'))
     AND upper(coalesce(mt.diagnosis_indicator_text, '1')) = upper(coalesce(ms.diagnosis_indicator_text, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (diagnosis_detail_id,
        diagnosis_detail_desc,
        diagnosis_indicator_text,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.diagnosis_detail_id, ms.diagnosis_detail_desc, ms.diagnosis_indicator_text, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT diagnosis_detail_id
      FROM {{ params.param_cr_core_dataset_name }}.ref_diagnosis_detail
      GROUP BY diagnosis_detail_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_cr_core_dataset_name }}.ref_diagnosis_detail');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','Ref_Diagnosis_Detail');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF