DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cr_patient_core.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #####################################################################################
-- #  TARGET TABLE		: EDWCR.CR_PATIENT             		#
-- #  TARGET  DATABASE	   	: EDWCR	 						#
-- #  SOURCE		   	: EDWCR_STAGING.CR_PATIENT_WRK	#
-- #	                                                                        	#
-- #  INITIAL RELEASE	   	: 								#
-- #  PROJECT             	: 	 		    					#
-- #  ------------------------------------------------------------------------		#
-- #                                                                              	#
-- #####################################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_PATIENT;;
 --' FOR SESSION;;
 /* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_STAGING','CR_PATIENT_WRK');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Truncate Core Table */ BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.cr_patient;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Populate Core Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cr_patient AS mt USING
  (SELECT DISTINCT cr_patient_wrk.cr_patient_id,
                   cr_patient_wrk.patient_gender_id,
                   cr_patient_wrk.patient_race_id,
                   cr_patient_wrk.vital_status_id,
                   cr_patient_wrk.patient_system_id,
                   cr_patient_wrk.coid AS coid,
                   cr_patient_wrk.company_code AS company_code,
                   cr_patient_wrk.patient_birth_date,
                   cr_patient_wrk.last_contact_date,
                   cr_patient_wrk.patient_first_name,
                   cr_patient_wrk.patient_middle_name,
                   cr_patient_wrk.patient_last_name,
                   cr_patient_wrk.patient_email_address_text,
                   cr_patient_wrk.accession_num_code AS accession_num_code,
                   CASE
                       WHEN upper(trim(cr_patient_wrk.patient_market_urn_text)) = '' THEN CAST(NULL AS STRING)
                       ELSE cr_patient_wrk.patient_market_urn_text
                   END AS patient_market_urn_text,
                   CASE
                       WHEN upper(trim(cr_patient_wrk.medical_record_num)) = '' THEN CAST(NULL AS STRING)
                       ELSE cr_patient_wrk.medical_record_num
                   END AS medical_record_num,
                   cr_patient_wrk.source_system_code AS source_system_code,
                   cr_patient_wrk.dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_wrk) AS ms ON mt.cr_patient_id = ms.cr_patient_id
AND (coalesce(mt.patient_gender_id, 0) = coalesce(ms.patient_gender_id, 0)
     AND coalesce(mt.patient_gender_id, 1) = coalesce(ms.patient_gender_id, 1))
AND (coalesce(mt.patient_race_id, 0) = coalesce(ms.patient_race_id, 0)
     AND coalesce(mt.patient_race_id, 1) = coalesce(ms.patient_race_id, 1))
AND (coalesce(mt.vital_status_id, 0) = coalesce(ms.vital_status_id, 0)
     AND coalesce(mt.vital_status_id, 1) = coalesce(ms.vital_status_id, 1))
AND (coalesce(mt.patient_system_id, 0) = coalesce(ms.patient_system_id, 0)
     AND coalesce(mt.patient_system_id, 1) = coalesce(ms.patient_system_id, 1))
AND mt.coid = ms.coid
AND mt.company_code = ms.company_code
AND (coalesce(mt.patient_birth_date, DATE '1970-01-01') = coalesce(ms.patient_birth_date, DATE '1970-01-01')
     AND coalesce(mt.patient_birth_date, DATE '1970-01-02') = coalesce(ms.patient_birth_date, DATE '1970-01-02'))
AND (coalesce(mt.last_contact_date, DATE '1970-01-01') = coalesce(ms.last_contact_date, DATE '1970-01-01')
     AND coalesce(mt.last_contact_date, DATE '1970-01-02') = coalesce(ms.last_contact_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.patient_first_name, '0')) = upper(coalesce(ms.patient_first_name, '0'))
     AND upper(coalesce(mt.patient_first_name, '1')) = upper(coalesce(ms.patient_first_name, '1')))
AND (upper(coalesce(mt.patient_middle_name, '0')) = upper(coalesce(ms.patient_middle_name, '0'))
     AND upper(coalesce(mt.patient_middle_name, '1')) = upper(coalesce(ms.patient_middle_name, '1')))
AND (upper(coalesce(mt.patient_last_name, '0')) = upper(coalesce(ms.patient_last_name, '0'))
     AND upper(coalesce(mt.patient_last_name, '1')) = upper(coalesce(ms.patient_last_name, '1')))
AND (upper(coalesce(mt.patient_email_address_text, '0')) = upper(coalesce(ms.patient_email_address_text, '0'))
     AND upper(coalesce(mt.patient_email_address_text, '1')) = upper(coalesce(ms.patient_email_address_text, '1')))
AND (upper(coalesce(mt.accession_num_code, '0')) = upper(coalesce(ms.accession_num_code, '0'))
     AND upper(coalesce(mt.accession_num_code, '1')) = upper(coalesce(ms.accession_num_code, '1')))
AND (upper(coalesce(mt.patient_market_urn_text, '0')) = upper(coalesce(ms.patient_market_urn_text, '0'))
     AND upper(coalesce(mt.patient_market_urn_text, '1')) = upper(coalesce(ms.patient_market_urn_text, '1')))
AND (upper(coalesce(mt.medical_record_num, '0')) = upper(coalesce(ms.medical_record_num, '0'))
     AND upper(coalesce(mt.medical_record_num, '1')) = upper(coalesce(ms.medical_record_num, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (cr_patient_id,
        patient_gender_id,
        patient_race_id,
        vital_status_id,
        patient_system_id,
        coid,
        company_code,
        patient_birth_date,
        last_contact_date,
        patient_first_name,
        patient_middle_name,
        patient_last_name,
        patient_email_address_text,
        accession_num_code,
        patient_market_urn_text,
        medical_record_num,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.cr_patient_id, ms.patient_gender_id, ms.patient_race_id, ms.vital_status_id, ms.patient_system_id, ms.coid, ms.company_code, ms.patient_birth_date, ms.last_contact_date, ms.patient_first_name, ms.patient_middle_name, ms.patient_last_name, ms.patient_email_address_text, ms.accession_num_code, ms.patient_market_urn_text, ms.medical_record_num, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT cr_patient_id
      FROM `hca-hin-dev-cur-ops`.edwcr.cr_patient
      GROUP BY cr_patient_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.cr_patient');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CR_PATIENT');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF