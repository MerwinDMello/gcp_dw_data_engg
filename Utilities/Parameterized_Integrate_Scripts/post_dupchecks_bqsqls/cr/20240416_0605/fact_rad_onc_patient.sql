DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/fact_rad_onc_patient.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.Fact_Rad_Onc_Patient                        	##
-- ##  TARGET  DATABASE	   : EDWCR	 											##
-- ##  SOURCE		  		   : EDWCR_Staging.STG_FactPatient 						##
-- ##														  						##
-- ##	                                                                        	##
-- ##  INITIAL RELEASE	   : 														##
-- ##  PROJECT            	   : 	 		    									##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_RO_FACT_RAD_ONC_PATIENT;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','STG_FactPatient');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- Deleting Data
BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.fact_rad_onc_patient;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.fact_rad_onc_patient AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY dhd.dimsiteid,
                                               dhd.factpatientid) AS fact_patient_sk,
                                     rpp.hospital_sk AS hospital_sk,
                                     rpa.patient_sk AS patient_sk,
                                     dhd.patient_status_id AS patient_status_id,
                                     ra.location_sk,
                                     dhd.race_id,
                                     dhd.gender_id,
                                     rr.site_sk AS site_sk,
                                     dhd.factpatientid AS source_fact_patient_id,
                                     dhd.creation_date_time,
                                     dhd.admission_date_time,
                                     dhd.discharge_date_time,
                                     dhd.log_id,
                                     dhd.run_id,
                                     'R' AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT DISTINCT stg_factpatient.dimsiteid AS dimsiteid,
                      stg_factpatient.factpatientid AS factpatientid,
                      stg_factpatient.dimhospitaldepartmentid,
                      stg_factpatient.dimpatientid,
                      stg_factpatient.dimlocationid,
                      stg_factpatient.dimlookupid_patientstatus AS patient_status_id,
                      stg_factpatient.dimlookupid_race AS race_id,
                      stg_factpatient.dimlookupid_gender AS gender_id,
                      CAST(trim(substr(stg_factpatient.patientcreationdate, 1, 19)) AS DATETIME) AS creation_date_time,
                      CAST(trim(substr(stg_factpatient.patientadmissiondate, 1, 19)) AS DATETIME) AS admission_date_time,
                      CAST(trim(substr(stg_factpatient.patientdischargedate, 1, 19)) AS DATETIME) AS discharge_date_time,
                      stg_factpatient.logid AS log_id,
                      stg_factpatient.runid AS run_id
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_factpatient) AS dhd
   INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_rad_onc_site AS rr ON rr.source_site_id = dhd.dimsiteid
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_rad_onc_location AS ra ON dhd.dimlocationid = ra.source_location_id
   AND rr.site_sk = ra.site_sk
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_rad_onc_hospital AS rpp ON dhd.dimhospitaldepartmentid = rpp.source_hospital_id
   AND rr.site_sk = rpp.site_sk
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.rad_onc_patient AS rpa ON dhd.dimpatientid = rpa.source_patient_id
   AND rr.site_sk = rpa.site_sk) AS ms ON mt.fact_patient_sk = ms.fact_patient_sk
AND (coalesce(mt.hospital_sk, 0) = coalesce(ms.hospital_sk, 0)
     AND coalesce(mt.hospital_sk, 1) = coalesce(ms.hospital_sk, 1))
AND (coalesce(mt.patient_sk, 0) = coalesce(ms.patient_sk, 0)
     AND coalesce(mt.patient_sk, 1) = coalesce(ms.patient_sk, 1))
AND (coalesce(mt.patient_status_id, 0) = coalesce(ms.patient_status_id, 0)
     AND coalesce(mt.patient_status_id, 1) = coalesce(ms.patient_status_id, 1))
AND (coalesce(mt.location_sk, 0) = coalesce(ms.location_sk, 0)
     AND coalesce(mt.location_sk, 1) = coalesce(ms.location_sk, 1))
AND (coalesce(mt.race_id, 0) = coalesce(ms.race_id, 0)
     AND coalesce(mt.race_id, 1) = coalesce(ms.race_id, 1))
AND (coalesce(mt.gender_id, 0) = coalesce(ms.gender_id, 0)
     AND coalesce(mt.gender_id, 1) = coalesce(ms.gender_id, 1))
AND mt.site_sk = ms.site_sk
AND mt.source_fact_patient_id = ms.source_fact_patient_id
AND (coalesce(mt.creation_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.creation_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.creation_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.creation_date_time, DATETIME '1970-01-01 00:00:01'))
AND (coalesce(mt.admission_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.admission_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.admission_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.admission_date_time, DATETIME '1970-01-01 00:00:01'))
AND (coalesce(mt.discharge_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.discharge_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.discharge_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.discharge_date_time, DATETIME '1970-01-01 00:00:01'))
AND (coalesce(mt.log_id, 0) = coalesce(ms.log_id, 0)
     AND coalesce(mt.log_id, 1) = coalesce(ms.log_id, 1))
AND (coalesce(mt.run_id, 0) = coalesce(ms.run_id, 0)
     AND coalesce(mt.run_id, 1) = coalesce(ms.run_id, 1))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (fact_patient_sk,
        hospital_sk,
        patient_sk,
        patient_status_id,
        location_sk,
        race_id,
        gender_id,
        site_sk,
        source_fact_patient_id,
        creation_date_time,
        admission_date_time,
        discharge_date_time,
        log_id,
        run_id,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.fact_patient_sk, ms.hospital_sk, ms.patient_sk, ms.patient_status_id, ms.location_sk, ms.race_id, ms.gender_id, ms.site_sk, ms.source_fact_patient_id, ms.creation_date_time, ms.admission_date_time, ms.discharge_date_time, ms.log_id, ms.run_id, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT fact_patient_sk
      FROM `hca-hin-dev-cur-ops`.edwcr.fact_rad_onc_patient
      GROUP BY fact_patient_sk
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.fact_rad_onc_patient');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','Fact_Rad_Onc_Patient');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF;;