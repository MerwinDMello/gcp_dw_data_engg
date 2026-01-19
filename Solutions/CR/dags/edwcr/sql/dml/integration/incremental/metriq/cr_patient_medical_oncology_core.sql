DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cr_patient_medical_oncology_core.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #########################################################################
-- #  TARGET TABLE		: EDWCR.CR_Patient_Medical_Oncology      	#
-- #  TARGET  DATABASE	   : EDWCR	 				#
-- #  SOURCE	: EDWCR_Staging.CR_Patient_Medical_Oncology_WRK	        #
-- #	                                                                #
-- #  INITIAL RELEASE	   	: 					#
-- #  PROJECT             	: 	 		    			#
-- #  ---------------------------------------------------------------------#
-- #                                                                       #
-- #########################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_PATIENT_MEDICAL_ONCOLOGY;;
 --' FOR SESSION;;
 /* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_STAGING','CR_Patient_Medical_Oncology_WRK');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Truncate Core Table */ BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE {{ params.param_cr_core_dataset_name }}.cr_patient_medical_oncology;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Populate Core Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_cr_core_dataset_name }}.cr_patient_medical_oncology AS mt USING
  (SELECT DISTINCT cr_patient_medical_oncology_wrk.cycle_id,
                   cr_patient_medical_oncology_wrk.treatment_id,
                   cr_patient_medical_oncology_wrk.drug_route_id,
                   cr_patient_medical_oncology_wrk.drug_dose_unit_id,
                   cr_patient_medical_oncology_wrk.drug_id,
                   cr_patient_medical_oncology_wrk.drug_days_given_num_text AS drug_days_given_num_text,
                   cr_patient_medical_oncology_wrk.drug_frequency_num,
                   cr_patient_medical_oncology_wrk.total_drug_dose_amt,
                   cr_patient_medical_oncology_wrk.drug_hospital_id,
                   cr_patient_medical_oncology_wrk.nsc_id,
                   cr_patient_medical_oncology_wrk.treatment_start_date,
                   cr_patient_medical_oncology_wrk.treatment_end_date,
                   cr_patient_medical_oncology_wrk.cycle_num_text AS cycle_num_text,
                   cr_patient_medical_oncology_wrk.source_system_code AS source_system_code,
                   cr_patient_medical_oncology_wrk.dw_last_update_date_time
   FROM {{ params.param_cr_stage_dataset_name }}.cr_patient_medical_oncology_wrk) AS ms ON coalesce(mt.cycle_id, 0) = coalesce(ms.cycle_id, 0)
AND coalesce(mt.cycle_id, 1) = coalesce(ms.cycle_id, 1)
AND (coalesce(mt.treatment_id, 0) = coalesce(ms.treatment_id, 0)
     AND coalesce(mt.treatment_id, 1) = coalesce(ms.treatment_id, 1))
AND (coalesce(mt.drug_route_id, 0) = coalesce(ms.drug_route_id, 0)
     AND coalesce(mt.drug_route_id, 1) = coalesce(ms.drug_route_id, 1))
AND (coalesce(mt.drug_dose_unit_id, 0) = coalesce(ms.drug_dose_unit_id, 0)
     AND coalesce(mt.drug_dose_unit_id, 1) = coalesce(ms.drug_dose_unit_id, 1))
AND mt.drug_id = ms.drug_id
AND (upper(coalesce(mt.drug_days_given_num_text, '0')) = upper(coalesce(ms.drug_days_given_num_text, '0'))
     AND upper(coalesce(mt.drug_days_given_num_text, '1')) = upper(coalesce(ms.drug_days_given_num_text, '1')))
AND (coalesce(mt.drug_frequency_num, NUMERIC '0') = coalesce(ms.drug_frequency_num, NUMERIC '0')
     AND coalesce(mt.drug_frequency_num, NUMERIC '1') = coalesce(ms.drug_frequency_num, NUMERIC '1'))
AND (coalesce(mt.total_drug_dose_amt, NUMERIC '0') = coalesce(ms.total_drug_dose_amt, NUMERIC '0')
     AND coalesce(mt.total_drug_dose_amt, NUMERIC '1') = coalesce(ms.total_drug_dose_amt, NUMERIC '1'))
AND (coalesce(mt.drug_hospital_id, 0) = coalesce(ms.drug_hospital_id, 0)
     AND coalesce(mt.drug_hospital_id, 1) = coalesce(ms.drug_hospital_id, 1))
AND (coalesce(mt.nsc_id, 0) = coalesce(ms.nsc_id, 0)
     AND coalesce(mt.nsc_id, 1) = coalesce(ms.nsc_id, 1))
AND (coalesce(mt.treatment_start_date, DATE '1970-01-01') = coalesce(ms.treatment_start_date, DATE '1970-01-01')
     AND coalesce(mt.treatment_start_date, DATE '1970-01-02') = coalesce(ms.treatment_start_date, DATE '1970-01-02'))
AND (coalesce(mt.treatment_end_date, DATE '1970-01-01') = coalesce(ms.treatment_end_date, DATE '1970-01-01')
     AND coalesce(mt.treatment_end_date, DATE '1970-01-02') = coalesce(ms.treatment_end_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.cycle_num_text, '0')) = upper(coalesce(ms.cycle_num_text, '0'))
     AND upper(coalesce(mt.cycle_num_text, '1')) = upper(coalesce(ms.cycle_num_text, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (cycle_id,
        treatment_id,
        drug_route_id,
        drug_dose_unit_id,
        drug_id,
        drug_days_given_num_text,
        drug_frequency_num,
        total_drug_dose_amt,
        drug_hospital_id,
        nsc_id,
        treatment_start_date,
        treatment_end_date,
        cycle_num_text,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.cycle_id, ms.treatment_id, ms.drug_route_id, ms.drug_dose_unit_id, ms.drug_id, ms.drug_days_given_num_text, ms.drug_frequency_num, ms.total_drug_dose_amt, ms.drug_hospital_id, ms.nsc_id, ms.treatment_start_date, ms.treatment_end_date, ms.cycle_num_text, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT drug_id
      FROM {{ params.param_cr_core_dataset_name }}.cr_patient_medical_oncology
      GROUP BY drug_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_cr_core_dataset_name }}.cr_patient_medical_oncology');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CR_Patient_Medical_Oncology');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF