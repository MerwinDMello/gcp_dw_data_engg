DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/j_ref_rad_onc_treatment_type.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.Ref_Rad_Onc_Treatment_type                                                ##
-- ##  TARGET  DATABASE	   : EDWCR	 					                                          ##
-- ##  SOURCE		  		   : EDWCR_STAGING.stg_SC_Modalities ##
-- ##							  		                                  ##
-- ##	                                                                                              ##
-- ##  INITIAL RELEASE	   : 							                                              ##
-- ##  PROJECT            	   : 	 		    				                                      ##
-- ##  ------------------------------------------------------------------------	                  ##
-- ##                                                                                                ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_RO_REF_RAD_ONC_ADDRESS;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','stg_SC_Modalities');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr_staging.stg_sc_modalities_wrk;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


INSERT INTO `hca-hin-dev-cur-ops`.edwcr_staging.stg_sc_modalities_wrk (treatment_type_sk, treatment_category_desc, treatment_type_desc, source_system_code, dw_last_update_date_time)
SELECT NULL AS treatment_type_sk,
       substr(trim(stg_sc_modalities.treatment_category), 1, 50) AS treatment_category_desc,
       substr(trim(stg_sc_modalities.treatment_type), 1, 50) AS treatment_type_desc,
       'R' AS source_system_code,
       datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_sc_modalities;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- Deleting Data
IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_treatment_type;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_treatment_type AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY upper(max(trim(sc.treatment_category_desc))),
                                               upper(max(trim(sc.treatment_type_desc)))) AS treatment_type_sk,
                                     substr(max(trim(sc.treatment_category_desc)), 1, 50) AS treatment_category_desc,
                                     substr(max(trim(sc.treatment_type_desc)), 1, 50) AS treatment_type_desc,
                                     'R' AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_sc_modalities_wrk AS sc
   WHERE sc.treatment_category_desc IS NOT NULL
     AND upper(rtrim(sc.treatment_category_desc)) <> ''
     AND sc.treatment_type_desc IS NOT NULL
     AND upper(rtrim(sc.treatment_type_desc)) <> ''
   GROUP BY upper(substr(trim(sc.treatment_category_desc), 1, 50)),
            upper(substr(trim(sc.treatment_type_desc), 1, 50))) AS ms ON mt.treatment_type_sk = ms.treatment_type_sk
AND (upper(coalesce(mt.treatment_category_desc, '0')) = upper(coalesce(ms.treatment_category_desc, '0'))
     AND upper(coalesce(mt.treatment_category_desc, '1')) = upper(coalesce(ms.treatment_category_desc, '1')))
AND (upper(coalesce(mt.treatment_type_desc, '0')) = upper(coalesce(ms.treatment_type_desc, '0'))
     AND upper(coalesce(mt.treatment_type_desc, '1')) = upper(coalesce(ms.treatment_type_desc, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (treatment_type_sk,
        treatment_category_desc,
        treatment_type_desc,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.treatment_type_sk, ms.treatment_category_desc, ms.treatment_type_desc, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT treatment_type_sk
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_treatment_type
      GROUP BY treatment_type_sk
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_treatment_type');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','Ref_Rad_Onc_Treatment_type');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF