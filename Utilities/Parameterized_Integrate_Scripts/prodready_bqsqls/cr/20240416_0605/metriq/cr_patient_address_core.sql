DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cr_patient_address_core.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #########################################################################
-- #  TARGET TABLE		: EDWCR.CR_PATIENT_ADDRESS             		#
-- #  TARGET  DATABASE	   	: EDWCR	 				#
-- #  SOURCE		   	: EDWCR_STAGING.CR_PATIENT_ADDRESS_WRK	#
-- #	                                                                #
-- #  INITIAL RELEASE	   	: Data upload to pateient address       #
-- #  PROJECT             	: 	 		    			#
-- #  ---------------------------------------------------------------------#
-- #                                                                       #
-- #########################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_PATIENT_ADDRESS;;
 --' FOR SESSION;;
 /* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_STAGING','CR_PATIENT_ADDRESS_WRK');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Truncate Core Table */ BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE {{ params.param_cr_core_dataset_name }}.cr_patient_address;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Populate Core Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_cr_core_dataset_name }}.cr_patient_address AS mt USING
  (SELECT DISTINCT cr_patient_address_wrk.cr_patient_id,
                   cr_patient_address_wrk.state_id,
                   cr_patient_address_wrk.address_line_1_text,
                   cr_patient_address_wrk.address_line_2_text,
                   cr_patient_address_wrk.city_name,
                   cr_patient_address_wrk.zip_code AS zip_code,
                   cr_patient_address_wrk.source_system_code AS source_system_code,
                   cr_patient_address_wrk.dw_last_update_date_time
   FROM {{ params.param_cr_stage_dataset_name }}.cr_patient_address_wrk) AS ms ON mt.cr_patient_id = ms.cr_patient_id
AND (coalesce(mt.state_id, 0) = coalesce(ms.state_id, 0)
     AND coalesce(mt.state_id, 1) = coalesce(ms.state_id, 1))
AND (upper(coalesce(mt.address_line_1_text, '0')) = upper(coalesce(ms.address_line_1_text, '0'))
     AND upper(coalesce(mt.address_line_1_text, '1')) = upper(coalesce(ms.address_line_1_text, '1')))
AND (upper(coalesce(mt.address_line_2_text, '0')) = upper(coalesce(ms.address_line_2_text, '0'))
     AND upper(coalesce(mt.address_line_2_text, '1')) = upper(coalesce(ms.address_line_2_text, '1')))
AND (upper(coalesce(mt.city_name, '0')) = upper(coalesce(ms.city_name, '0'))
     AND upper(coalesce(mt.city_name, '1')) = upper(coalesce(ms.city_name, '1')))
AND (upper(coalesce(mt.zip_code, '0')) = upper(coalesce(ms.zip_code, '0'))
     AND upper(coalesce(mt.zip_code, '1')) = upper(coalesce(ms.zip_code, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (cr_patient_id,
        state_id,
        address_line_1_text,
        address_line_2_text,
        city_name,
        zip_code,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.cr_patient_id, ms.state_id, ms.address_line_1_text, ms.address_line_2_text, ms.city_name, ms.zip_code, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT cr_patient_id
      FROM {{ params.param_cr_core_dataset_name }}.cr_patient_address
      GROUP BY cr_patient_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_cr_core_dataset_name }}.cr_patient_address');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CR_PATIENT_ADDRESS');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF