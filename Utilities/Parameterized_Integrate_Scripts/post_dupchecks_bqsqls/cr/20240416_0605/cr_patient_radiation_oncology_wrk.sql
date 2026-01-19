DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cr_patient_radiation_oncology_wrk.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #####################################################################################
-- #  TARGET TABLE		: EDWCR_STAGING.CR_Patient_Radiation_Oncology_WRK           #
-- #  TARGET  DATABASE	   	: EDWCR_STAGING	 				    #
-- #  SOURCE		   	: EDWCR_STAGING.CR_Patient_Radiation_Oncology_STG   #
-- #	                                                                            #
-- #  INITIAL RELEASE	   	: 						    #
-- #  PROJECT             	: 	 		    				    #
-- #  ------------------------------------------------------------------------	    #
-- #                                                                              	    #
-- #####################################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_PATIENT_RADIATION_ONCOLOGY;;
 --' FOR SESSION;;
 /* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_STAGING','CR_Patient_Radiation_Oncology_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Truncate WRK Table */ BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_radiation_oncology_wrk;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Populate WRK Table */ BEGIN
SET _ERROR_CODE = 0;


INSERT INTO `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_radiation_oncology_wrk (radiation_id, treatment_id, radiation_type_id, radiation_hospital_id, radiation_treatment_start_date, radiation_treatment_end_date, source_system_code, dw_last_update_date_time)
SELECT src.radiation_id,
       src.treatment_id,
       coalesce(rlc.master_lookup_sid,
                  (SELECT rcl.master_lookup_sid
                   FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rcl
                   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS nm ON rcl.lookup_id = nm.lookup_sid
                   WHERE upper(rtrim(nm.lookup_name)) = 'RADIATION TYPE'
                     AND upper(rtrim(rcl.lookup_code)) = '-99' )) AS radiation_type_id,
       a.hospital_id AS radiation_hospital_id,
       src.radiation_treatment_start_date,
       src.radiation_treatment_end_date,
       src.source_system_code,
       src.dw_last_update_date_time
FROM
  (SELECT stg.radiation_id AS radiation_id,
          stg.treatment_id AS treatment_id,
          stg.radiation_type_id AS radiation_type_id,
          stg.radiation_hospital_id AS radiation_hospital_id,
          stg.radiation_treatment_start_date AS radiation_treatment_start_date,
          stg.radiation_treatment_end_date AS radiation_treatment_end_date,
          stg.source_system_code AS source_system_code,
          stg.dw_last_update_date_time AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_radiation_oncology_stg AS stg) AS src
LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_hospital AS a ON upper(rtrim(src.radiation_hospital_id)) = upper(rtrim(a.hospital_code))
LEFT OUTER JOIN
  (SELECT lkp.lookup_code,
          lkp.master_lookup_sid
   FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS lkp
   INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS nm ON lkp.lookup_id = nm.lookup_sid
   WHERE upper(rtrim(nm.lookup_name)) = 'RADIATION TYPE' ) AS rlc ON upper(rtrim(src.radiation_type_id)) = upper(rtrim(rlc.lookup_code));


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_STAGING','CR_Patient_Radiation_Oncology_WRK');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF;;