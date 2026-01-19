DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cr_patient_radiation_oncology_core.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #########################################################################
-- #  TARGET TABLE		: EDWCR.CR_Patient_Radiation_Oncology      	#
-- #  TARGET  DATABASE	   : EDWCR	 				#
-- #  SOURCE	: EDWCR_Staging.CR_Patient_Radiation_Oncology_WRK	#
-- #	                                                                #
-- #  INITIAL RELEASE	   	: 					#
-- #  PROJECT             	: 	 		    			#
-- #  ---------------------------------------------------------------------#
-- #                                                                       #
-- #########################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_PATIENT_RADIATION_ONCOLOGY;;
 --' FOR SESSION;;
 /* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_STAGING','CR_Patient_Radiation_Oncology_WRK');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Truncate Core Table */ BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.cr_patient_radiation_oncology;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Populate Core Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cr_patient_radiation_oncology AS mt USING
  (SELECT DISTINCT cr_patient_radiation_oncology_wrk.radiation_id,
                   cr_patient_radiation_oncology_wrk.treatment_id,
                   cr_patient_radiation_oncology_wrk.radiation_type_id,
                   cr_patient_radiation_oncology_wrk.radiation_hospital_id,
                   cr_patient_radiation_oncology_wrk.radiation_treatment_start_date,
                   cr_patient_radiation_oncology_wrk.radiation_treatment_end_date,
                   cr_patient_radiation_oncology_wrk.source_system_code AS source_system_code,
                   cr_patient_radiation_oncology_wrk.dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_radiation_oncology_wrk) AS ms ON mt.radiation_id = ms.radiation_id
AND (coalesce(mt.treatment_id, 0) = coalesce(ms.treatment_id, 0)
     AND coalesce(mt.treatment_id, 1) = coalesce(ms.treatment_id, 1))
AND (coalesce(mt.radiation_type_id, 0) = coalesce(ms.radiation_type_id, 0)
     AND coalesce(mt.radiation_type_id, 1) = coalesce(ms.radiation_type_id, 1))
AND (coalesce(mt.radiation_hospital_id, 0) = coalesce(ms.radiation_hospital_id, 0)
     AND coalesce(mt.radiation_hospital_id, 1) = coalesce(ms.radiation_hospital_id, 1))
AND (coalesce(mt.radiation_treatment_start_date, DATE '1970-01-01') = coalesce(ms.radiation_treatment_start_date, DATE '1970-01-01')
     AND coalesce(mt.radiation_treatment_start_date, DATE '1970-01-02') = coalesce(ms.radiation_treatment_start_date, DATE '1970-01-02'))
AND (coalesce(mt.radiation_treatment_end_date, DATE '1970-01-01') = coalesce(ms.radiation_treatment_end_date, DATE '1970-01-01')
     AND coalesce(mt.radiation_treatment_end_date, DATE '1970-01-02') = coalesce(ms.radiation_treatment_end_date, DATE '1970-01-02'))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (radiation_id,
        treatment_id,
        radiation_type_id,
        radiation_hospital_id,
        radiation_treatment_start_date,
        radiation_treatment_end_date,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.radiation_id, ms.treatment_id, ms.radiation_type_id, ms.radiation_hospital_id, ms.radiation_treatment_start_date, ms.radiation_treatment_end_date, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT radiation_id
      FROM `hca-hin-dev-cur-ops`.edwcr.cr_patient_radiation_oncology
      GROUP BY radiation_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.cr_patient_radiation_oncology');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CR_Patient_Radiation_Oncology');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF