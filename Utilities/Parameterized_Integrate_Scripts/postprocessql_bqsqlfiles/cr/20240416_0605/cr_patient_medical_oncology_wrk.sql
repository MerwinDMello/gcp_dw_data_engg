DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cr_patient_medical_oncology_wrk.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #####################################################################################
-- #  TARGET TABLE		: EDWCR_STAGING.CR_Patient_Medical_Oncology_WRK             #
-- #  TARGET  DATABASE	   	: EDWCR_STAGING	 				    #
-- #  SOURCE		   	: EDWCR_STAGING.CR_PATIENT_MEDICAL_ONCOLOGY_STG     #
-- #	                                                                            #
-- #  INITIAL RELEASE	   	: 						    #
-- #  PROJECT             	: 	 		    				    #
-- #  ------------------------------------------------------------------------	    #
-- #                                                                              	    #
-- #####################################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_PATIENT_MEDICAL_ONCOLOGY;;
 --' FOR SESSION;;
 /* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_STAGING','CR_Patient_Medical_Oncology_WRK');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Truncate WRK Table */ BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_medical_oncology_wrk;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Populate WRK Table */ BEGIN
SET _ERROR_CODE = 0;


INSERT INTO `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_medical_oncology_wrk (cycle_id, treatment_id, drug_route_id, drug_dose_unit_id, drug_id, drug_days_given_num_text, drug_frequency_num, total_drug_dose_amt, drug_hospital_id, nsc_id, treatment_start_date, treatment_end_date, cycle_num_text, source_system_code, dw_last_update_date_time)
SELECT DISTINCT stg.cycle_id,
                stg.treatment_id,
                CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(CASE
                                                                                 WHEN route.master_lookup_sid IS NULL THEN '-99'
                                                                                 ELSE format('%11d', route.master_lookup_sid)
                                                                             END) AS INT64) AS drug_route_id,
                CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(CASE
                                                                                 WHEN dose.master_lookup_sid IS NULL THEN '-99'
                                                                                 ELSE format('%11d', dose.master_lookup_sid)
                                                                             END) AS INT64) AS drug_dose_unit_id,
                stg.drug_id,
                stg.drug_days_given_num_text,
                stg.drug_frequency_num,
                stg.total_drug_dose_amt,
                refh.hospital_id AS drug_hospital_id,
                rnsc.nsc_id,
                stg.treatment_start_date,
                stg.treatment_end_date,
                stg.cycle_num_text,
                stg.source_system_code,
                stg.dw_last_update_date_time
FROM `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_medical_oncology_stg AS stg
LEFT OUTER JOIN
  (SELECT refc.master_lookup_sid,
          refc.lookup_code
   FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS refc
   INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS refn ON refc.lookup_id = refn.lookup_sid
   AND upper(rtrim(refn.lookup_name)) = 'DRUG ROUTE') AS route ON upper(rtrim(route.lookup_code)) = upper(rtrim(stg.drug_route_id))
LEFT OUTER JOIN
  (SELECT refc.master_lookup_sid,
          refc.lookup_code
   FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS refc
   INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS refn ON refc.lookup_id = refn.lookup_sid
   AND upper(rtrim(refn.lookup_name)) = 'DOSE UNITS') AS dose ON upper(rtrim(dose.lookup_code)) = upper(rtrim(stg.drug_dose_unit_id))
LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_hospital AS refh ON upper(rtrim(refh.hospital_code)) = upper(rtrim(stg.drug_hospital_id))
LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_national_service_center AS rnsc ON upper(rtrim(rnsc.nsc_code)) = upper(rtrim(stg.nsc_id))
AND upper(rtrim(rnsc.nsc_sub_code)) = upper(rtrim(stg.nsc_subcode));


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_STAGING','CR_Patient_Medical_Oncology_WRK');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF