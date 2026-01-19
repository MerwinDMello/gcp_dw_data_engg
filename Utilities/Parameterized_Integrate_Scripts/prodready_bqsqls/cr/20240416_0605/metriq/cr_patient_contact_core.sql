DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cr_patient_contact_core.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #########################################################################
-- #  TARGET TABLE		: EDWCR.CR_PATIENT_CONTACT             		#
-- #  TARGET  DATABASE	   	: EDWCR	 				#
-- #  SOURCE		   	: EDWCR_STAGING.CR_PATIENT_CONTACT_WRK	#
-- #	                                                                #
-- #  INITIAL RELEASE	   	: 					#
-- #  PROJECT             	: 	 		    			#
-- #  ---------------------------------------------------------------------#
-- #                                                                       #
-- #########################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_PATIENT_CONTACT;;
 --' FOR SESSION;;
 /* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_STAGING','CR_PATIENT_CONTACT_WRK');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Truncate Core Table */ BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE {{ params.param_cr_core_dataset_name }}.cr_patient_contact;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Populate Core Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_cr_core_dataset_name }}.cr_patient_contact AS mt USING
  (SELECT DISTINCT cr_patient_contact_wrk.patient_contact_id,
                   cr_patient_contact_wrk.cr_patient_id,
                   cr_patient_contact_wrk.contact_relation_id,
                   cr_patient_contact_wrk.contact_type_id,
                   cr_patient_contact_wrk.contact_num_code AS contact_num_code,
                   cr_patient_contact_wrk.contact_first_name,
                   cr_patient_contact_wrk.contact_last_name,
                   cr_patient_contact_wrk.contact_middle_name,
                   cr_patient_contact_wrk.preferred_contact_method_text,
                   cr_patient_contact_wrk.source_system_code AS source_system_code,
                   cr_patient_contact_wrk.dw_last_update_date_time
   FROM {{ params.param_cr_stage_dataset_name }}.cr_patient_contact_wrk) AS ms ON mt.patient_contact_id = ms.patient_contact_id
AND mt.cr_patient_id = ms.cr_patient_id
AND (coalesce(mt.contact_relation_id, 0) = coalesce(ms.contact_relation_id, 0)
     AND coalesce(mt.contact_relation_id, 1) = coalesce(ms.contact_relation_id, 1))
AND (coalesce(mt.contact_type_id, 0) = coalesce(ms.contact_type_id, 0)
     AND coalesce(mt.contact_type_id, 1) = coalesce(ms.contact_type_id, 1))
AND (upper(coalesce(mt.contact_num_code, '0')) = upper(coalesce(ms.contact_num_code, '0'))
     AND upper(coalesce(mt.contact_num_code, '1')) = upper(coalesce(ms.contact_num_code, '1')))
AND (upper(coalesce(mt.contact_first_name, '0')) = upper(coalesce(ms.contact_first_name, '0'))
     AND upper(coalesce(mt.contact_first_name, '1')) = upper(coalesce(ms.contact_first_name, '1')))
AND (upper(coalesce(mt.contact_last_name, '0')) = upper(coalesce(ms.contact_last_name, '0'))
     AND upper(coalesce(mt.contact_last_name, '1')) = upper(coalesce(ms.contact_last_name, '1')))
AND (upper(coalesce(mt.contact_middle_name, '0')) = upper(coalesce(ms.contact_middle_name, '0'))
     AND upper(coalesce(mt.contact_middle_name, '1')) = upper(coalesce(ms.contact_middle_name, '1')))
AND (upper(coalesce(mt.preferred_contact_method_text, '0')) = upper(coalesce(ms.preferred_contact_method_text, '0'))
     AND upper(coalesce(mt.preferred_contact_method_text, '1')) = upper(coalesce(ms.preferred_contact_method_text, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (patient_contact_id,
        cr_patient_id,
        contact_relation_id,
        contact_type_id,
        contact_num_code,
        contact_first_name,
        contact_last_name,
        contact_middle_name,
        preferred_contact_method_text,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.patient_contact_id, ms.cr_patient_id, ms.contact_relation_id, ms.contact_type_id, ms.contact_num_code, ms.contact_first_name, ms.contact_last_name, ms.contact_middle_name, ms.preferred_contact_method_text, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT patient_contact_id,
             cr_patient_id
      FROM {{ params.param_cr_core_dataset_name }}.cr_patient_contact
      GROUP BY patient_contact_id,
               cr_patient_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_cr_core_dataset_name }}.cr_patient_contact');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CR_PATIENT_CONTACT');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF