-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/ref_rad_onc_patient.sql
-- Translated from: bteq
-- Translated to: BigQuery

DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MSG STRING DEFAULT '';
-- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.Rad_Onc_Patient                          	##
-- ##  TARGET  DATABASE	   : EDWCR	 											##
-- ##  SOURCE		  		   : EDWCR_Staging.stg_DIMPATIENT 						##
-- ##							,EDWCR_STAGING.STG_DIMDOCTOR  						##
-- ##	                                                                        	##
-- ##  INITIAL RELEASE	   : 														##
-- ##  PROJECT            	   : 	 		    									##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;
-- SET QUERY_BAND = 'App=EDWCR_ETL; Job=J_CR_RO_REF_RAD_ONC_PATIENT;' FOR SESSION;
-- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','stg_DIMPATIENT');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- Deleting Data
BEGIN
  SET _ERROR_CODE = 0;
  TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.rad_onc_patient;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
BEGIN
  SET _ERROR_CODE = 0;
  MERGE INTO `hca-hin-dev-cur-ops`.edwcr.rad_onc_patient AS mt USING (
    SELECT DISTINCT
        row_number() OVER (ORDER BY dhd.dimsiteid, dhd.dimpatientid) AS patient_sk,
        ra.address_sk AS patient_address_sk,
        rr.site_sk AS site_sk,
        dhd.dimpatientid AS source_patient_id,
        substr(CASE
          WHEN upper(trim(dhd.patientid)) = '' THEN CAST(NULL as STRING)
          ELSE trim(dhd.patientid)
        END, 1, 25) AS medical_record_num,
        CAST(trim(substr(dhd.patientdateofbirth, 1, 19)) as DATETIME) AS patient_birth_date_time,
        substr(CASE
          WHEN upper(trim(dhd.patientfirstname)) = '' THEN CAST(NULL as STRING)
          ELSE trim(dhd.patientfirstname)
        END, 1, 50) AS patient_first_name,
        substr(CASE
          WHEN upper(trim(dhd.patientmiddlename)) = '' THEN CAST(NULL as STRING)
          ELSE trim(dhd.patientmiddlename)
        END, 1, 50) AS patient_middle_name,
        substr(CASE
          WHEN upper(trim(dhd.patientlastname)) = '' THEN CAST(NULL as STRING)
          ELSE trim(dhd.patientlastname)
        END, 1, 50) AS patient_last_name,
        substr(CASE
          WHEN upper(trim(dhd.patienthonorific)) = '' THEN CAST(NULL as STRING)
          ELSE trim(dhd.patienthonorific)
        END, 1, 10) AS patient_title_name,
        substr(CASE
          WHEN upper(trim(dhd.patientemailaddress)) = '' THEN CAST(NULL as STRING)
          ELSE trim(dhd.patientemailaddress)
        END, 1, 100) AS patient_email_address_text,
        CASE
           upper(trim(dhd.patientinoutstatus))
          WHEN 'IN' THEN 'I'
          WHEN 'OUT' THEN 'O'
          ELSE 'U'
        END AS patient_in_out_ind,
        CASE
           upper(trim(dhd.patientdeathstatus))
          WHEN 'ALIVE' THEN 'Y'
          WHEN 'DEAD' THEN 'N'
          ELSE 'U'
        END AS patient_death_ind,
        DATE(CAST(trim(substr(dhd.patientdeathdate, 1, 19)) as DATETIME)) AS patient_death_date,
        substr(CASE
          WHEN upper(trim(dhd.patientdeathcause)) = '' THEN CAST(NULL as STRING)
          ELSE trim(dhd.patientdeathcause)
        END, 1, 255) AS patient_death_reason_text,
        CASE
           upper(trim(dhd.clinicaltrial))
          WHEN 'YES' THEN 'Y'
          WHEN 'NO' THEN 'N'
          ELSE 'U'
        END AS clinical_trial_ind,
        substr(CASE
          WHEN upper(trim(dhd.transportationname)) = '' THEN CAST(NULL as STRING)
          ELSE trim(dhd.transportationname)
        END, 1, 100) AS patient_transportation_text,
        substr(CASE
          WHEN upper(trim(dhd.universalpatientid)) = '' THEN CAST(NULL as STRING)
          ELSE trim(dhd.universalpatientid)
        END, 1, 30) AS patient_global_unique_id_text,
        substr(CASE
          WHEN upper(trim(dhd.patientroomnumber)) = '' THEN CAST(NULL as STRING)
          ELSE trim(dhd.patientroomnumber)
        END, 1, 30) AS patient_room_number_text,
        CASE
           upper(trim(dhd.patienttype))
          WHEN 'ACTIVEPATIENT' THEN 'Y'
          WHEN 'DELETEDPATIENT' THEN 'N'
          ELSE 'U'
        END AS active_ind,
        substr(CASE
          WHEN upper(trim(dhd.patientlanguage)) = '' THEN CAST(NULL as STRING)
          ELSE trim(dhd.patientlanguage)
        END, 1, 50) AS patient_language_text,
        substr(CASE
          WHEN upper(trim(dhd.patientnotes)) = '' THEN CAST(NULL as STRING)
          ELSE trim(dhd.patientnotes)
        END, 1, 2000) AS patient_notes_text,
        dhd.logid AS log_id,
        dhd.runid AS run_id,
        substr(CASE
          WHEN upper(trim(dhd.hstryusername)) = '' THEN CAST(NULL as STRING)
          ELSE trim(dhd.hstryusername)
        END, 1, 30) AS history_user_name,
        CAST(trim(substr(dhd.hstrydatetime, 1, 19)) as DATETIME) AS history_date_time,
        'R' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimpatient AS dhd
        LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.rad_onc_address AS ra ON upper(rtrim(coalesce(trim(dhd.patientaddressline1), '###'))) = upper(rtrim(coalesce(trim(ra.address_line_1_text), '###')))
         AND upper(rtrim(coalesce(trim(dhd.patientaddressline2), '###'))) = upper(rtrim(coalesce(trim(ra.address_line_2_text), '###')))
         AND upper(rtrim(coalesce(trim(dhd.patientfulladdress), '###'))) = upper(rtrim(coalesce(trim(ra.full_address_text), '###')))
         AND upper(rtrim(coalesce(trim(dhd.patientaddresscomment), '###'))) = upper(rtrim(coalesce(trim(ra.address_comment_text), '###')))
        LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_rad_onc_site AS rr ON rr.source_site_id = dhd.dimsiteid
  ) AS ms
  ON mt.patient_sk = ms.patient_sk
   AND (coalesce(mt.patient_address_sk, 0) = coalesce(ms.patient_address_sk, 0)
   AND coalesce(mt.patient_address_sk, 1) = coalesce(ms.patient_address_sk, 1))
   AND mt.site_sk = ms.site_sk
   AND mt.source_patient_id = ms.source_patient_id
   AND mt.medical_record_num = ms.medical_record_num
   AND (coalesce(mt.patient_birth_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.patient_birth_date_time, DATETIME '1970-01-01 00:00:00')
   AND coalesce(mt.patient_birth_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.patient_birth_date_time, DATETIME '1970-01-01 00:00:01'))
   AND (upper(coalesce(mt.patient_first_name, '0')) = upper(coalesce(ms.patient_first_name, '0'))
   AND upper(coalesce(mt.patient_first_name, '1')) = upper(coalesce(ms.patient_first_name, '1')))
   AND (upper(coalesce(mt.patient_middle_name, '0')) = upper(coalesce(ms.patient_middle_name, '0'))
   AND upper(coalesce(mt.patient_middle_name, '1')) = upper(coalesce(ms.patient_middle_name, '1')))
   AND (upper(coalesce(mt.patient_last_name, '0')) = upper(coalesce(ms.patient_last_name, '0'))
   AND upper(coalesce(mt.patient_last_name, '1')) = upper(coalesce(ms.patient_last_name, '1')))
   AND (upper(coalesce(mt.patient_title_name, '0')) = upper(coalesce(ms.patient_title_name, '0'))
   AND upper(coalesce(mt.patient_title_name, '1')) = upper(coalesce(ms.patient_title_name, '1')))
   AND (upper(coalesce(mt.patient_email_address_text, '0')) = upper(coalesce(ms.patient_email_address_text, '0'))
   AND upper(coalesce(mt.patient_email_address_text, '1')) = upper(coalesce(ms.patient_email_address_text, '1')))
   AND (upper(coalesce(mt.patient_in_out_ind, '0')) = upper(coalesce(ms.patient_in_out_ind, '0'))
   AND upper(coalesce(mt.patient_in_out_ind, '1')) = upper(coalesce(ms.patient_in_out_ind, '1')))
   AND (upper(coalesce(mt.patient_death_ind, '0')) = upper(coalesce(ms.patient_death_ind, '0'))
   AND upper(coalesce(mt.patient_death_ind, '1')) = upper(coalesce(ms.patient_death_ind, '1')))
   AND (coalesce(mt.patient_death_date, DATE '1970-01-01') = coalesce(ms.patient_death_date, DATE '1970-01-01')
   AND coalesce(mt.patient_death_date, DATE '1970-01-02') = coalesce(ms.patient_death_date, DATE '1970-01-02'))
   AND (upper(coalesce(mt.patient_death_reason_text, '0')) = upper(coalesce(ms.patient_death_reason_text, '0'))
   AND upper(coalesce(mt.patient_death_reason_text, '1')) = upper(coalesce(ms.patient_death_reason_text, '1')))
   AND (upper(coalesce(mt.clinical_trial_ind, '0')) = upper(coalesce(ms.clinical_trial_ind, '0'))
   AND upper(coalesce(mt.clinical_trial_ind, '1')) = upper(coalesce(ms.clinical_trial_ind, '1')))
   AND (upper(coalesce(mt.patient_transportation_text, '0')) = upper(coalesce(ms.patient_transportation_text, '0'))
   AND upper(coalesce(mt.patient_transportation_text, '1')) = upper(coalesce(ms.patient_transportation_text, '1')))
   AND (upper(coalesce(mt.patient_global_unique_id_text, '0')) = upper(coalesce(ms.patient_global_unique_id_text, '0'))
   AND upper(coalesce(mt.patient_global_unique_id_text, '1')) = upper(coalesce(ms.patient_global_unique_id_text, '1')))
   AND (upper(coalesce(mt.patient_room_number_text, '0')) = upper(coalesce(ms.patient_room_number_text, '0'))
   AND upper(coalesce(mt.patient_room_number_text, '1')) = upper(coalesce(ms.patient_room_number_text, '1')))
   AND (upper(coalesce(mt.active_ind, '0')) = upper(coalesce(ms.active_ind, '0'))
   AND upper(coalesce(mt.active_ind, '1')) = upper(coalesce(ms.active_ind, '1')))
   AND (upper(coalesce(mt.patient_language_text, '0')) = upper(coalesce(ms.patient_language_text, '0'))
   AND upper(coalesce(mt.patient_language_text, '1')) = upper(coalesce(ms.patient_language_text, '1')))
   AND (upper(coalesce(mt.patient_notes_text, '0')) = upper(coalesce(ms.patient_notes_text, '0'))
   AND upper(coalesce(mt.patient_notes_text, '1')) = upper(coalesce(ms.patient_notes_text, '1')))
   AND (coalesce(mt.log_id, 0) = coalesce(ms.log_id, 0)
   AND coalesce(mt.log_id, 1) = coalesce(ms.log_id, 1))
   AND (coalesce(mt.run_id, 0) = coalesce(ms.run_id, 0)
   AND coalesce(mt.run_id, 1) = coalesce(ms.run_id, 1))
   AND (upper(coalesce(mt.history_user_name, '0')) = upper(coalesce(ms.history_user_name, '0'))
   AND upper(coalesce(mt.history_user_name, '1')) = upper(coalesce(ms.history_user_name, '1')))
   AND (coalesce(mt.history_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.history_date_time, DATETIME '1970-01-01 00:00:00')
   AND coalesce(mt.history_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.history_date_time, DATETIME '1970-01-01 00:00:01'))
   AND mt.source_system_code = ms.source_system_code
   AND mt.dw_last_update_date_time = ms.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN
      INSERT (patient_sk, patient_address_sk, site_sk, source_patient_id, medical_record_num, patient_birth_date_time, patient_first_name, patient_middle_name, patient_last_name, patient_title_name, patient_email_address_text, patient_in_out_ind, patient_death_ind, patient_death_date, patient_death_reason_text, clinical_trial_ind, patient_transportation_text, patient_global_unique_id_text, patient_room_number_text, active_ind, patient_language_text, patient_notes_text, log_id, run_id, history_user_name, history_date_time, source_system_code, dw_last_update_date_time)
      VALUES (ms.patient_sk, ms.patient_address_sk, ms.site_sk, ms.source_patient_id, ms.medical_record_num, ms.patient_birth_date_time, ms.patient_first_name, ms.patient_middle_name, ms.patient_last_name, ms.patient_title_name, ms.patient_email_address_text, ms.patient_in_out_ind, ms.patient_death_ind, ms.patient_death_date, ms.patient_death_reason_text, ms.clinical_trial_ind, ms.patient_transportation_text, ms.patient_global_unique_id_text, ms.patient_room_number_text, ms.active_ind, ms.patient_language_text, ms.patient_notes_text, ms.log_id, ms.run_id, ms.history_user_name, ms.history_date_time, ms.source_system_code, ms.dw_last_update_date_time)
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','Rad_Onc_Patient');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- EOF
