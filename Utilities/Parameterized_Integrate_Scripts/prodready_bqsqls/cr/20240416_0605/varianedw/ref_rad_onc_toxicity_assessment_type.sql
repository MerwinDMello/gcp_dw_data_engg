DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/ref_rad_onc_toxicity_assessment_type.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.Ref_Rad_Onc_Toxicity_Assessment_Type               ##
-- ##  TARGET  DATABASE	   : EDWCR	 											##
-- ##  SOURCE		   : EDWCR_STAGING.stg_FACTPATIENTTOXICITY 			##
-- ##														  						##
-- ##	                                                                        	##
-- ##  INITIAL RELEASE	   : 														##
-- ##  PROJECT            	   : 	 		    									##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_RO_RAD_ONC_PLAN_PURPOSE;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','stg_FACTPATIENTTOXICITY');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- Deleting Data
BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE {{ params.param_cr_core_dataset_name }}.ref_rad_onc_toxicity_assessment_type;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_cr_core_dataset_name }}.ref_rad_onc_toxicity_assessment_type AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY upper(src.toxicitycomponentname)) AS toxicity_assessment_type_sk,
                                     substr(src.toxicity_assessment_type_desc, 1, 50) AS toxicity_assessment_type_desc,
                                     'R' AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT CASE
                 WHEN upper(trim(dhd.toxicityassessmenttype)) = '' THEN CAST(NULL AS STRING)
                 ELSE trim(dhd.toxicityassessmenttype)
             END AS toxicity_assessment_type_desc,
             dhd.toxicitycomponentname
      FROM {{ params.param_cr_stage_dataset_name }}.stg_factpatienttoxicity AS dhd QUALIFY row_number() OVER (PARTITION BY upper(toxicity_assessment_type_desc)
                                                                                                              ORDER BY upper(toxicity_assessment_type_desc)) = 1) AS src) AS ms ON mt.toxicity_assessment_type_sk = ms.toxicity_assessment_type_sk
AND (upper(coalesce(mt.toxicity_assessment_type_desc, '0')) = upper(coalesce(ms.toxicity_assessment_type_desc, '0'))
     AND upper(coalesce(mt.toxicity_assessment_type_desc, '1')) = upper(coalesce(ms.toxicity_assessment_type_desc, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (toxicity_assessment_type_sk,
        toxicity_assessment_type_desc,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.toxicity_assessment_type_sk, ms.toxicity_assessment_type_desc, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT toxicity_assessment_type_sk
      FROM {{ params.param_cr_core_dataset_name }}.ref_rad_onc_toxicity_assessment_type
      GROUP BY toxicity_assessment_type_sk
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_cr_core_dataset_name }}.ref_rad_onc_toxicity_assessment_type');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','Ref_Rad_Onc_Toxicity_Assessment_Type');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF