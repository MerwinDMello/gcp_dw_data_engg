-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/rad_onc_activity_transaction.sql
-- Translated from: bteq
-- Translated to: BigQuery

DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MSG STRING DEFAULT '';
-- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.Rad_Onc_Activity_Transaction                        	##
-- ##  TARGET  DATABASE	   : EDWCR	 											##
-- ##  SOURCE		  		   : EDWCR_Staging.stg_DimActivityTransaction 						##
-- ##														  						##
-- ##	                                                                        	##
-- ##  INITIAL RELEASE	   : 														##
-- ##  PROJECT            	   : 	 		    									##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;
-- SET QUERY_BAND = 'App=EDWCR_ETL; Job=J_CR_RO_Rad_Onc_Activity_Transaction;' FOR SESSION;
-- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','stg_DimActivityTransaction');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- Deleting Data
BEGIN
  SET _ERROR_CODE = 0;
  TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.rad_onc_activity_transaction;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
BEGIN
  SET _ERROR_CODE = 0;
  MERGE INTO `hca-hin-dev-cur-ops`.edwcr.rad_onc_activity_transaction AS mt USING (
    SELECT DISTINCT
        row_number() OVER (ORDER BY upper(dp.dimsiteid), upper(dp.dimactivityid) DESC) AS fact_patient_toxicity_sk,
        ap.activity_priority_sk AS activity_priority_sk,
        rh.hospital_sk AS hospital_sk,
        rp.patient_sk AS patient_sk,
        ra.activity_sk AS activity_sk,
        CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dp.dimlookupid_appointmentstatus) as INT64) AS appointment_status_id,
        CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dp.dimlookupid_actualresourcetype) as INT64) AS actual_resource_type_id,
        CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dp.dimlookupid_cancelreasontype) as INT64) AS cancel_reason_type_id,
        CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dp.dimlookupid_appointmentrsrcsta) as INT64) AS appointment_resource_status_id,
        rr.site_sk AS site_sk,
        CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dp.dimactivitytransactionid) as INT64) AS source_activity_transaction_id,
        CAST(trim(substr(dp.scheduledendtime, 1, 19)) as DATETIME) AS schedule_end_date_time,
        CAST(trim(substr(dp.appointmentdatetime, 1, 19)) as DATETIME) AS appointment_date_time,
        dp.isscheduled AS appointment_schedule_ind,
        CAST(trim(substr(dp.activitystartdatetime, 1, 19)) as DATETIME) AS activity_start_date_time,
        CAST(trim(substr(dp.activityenddatetime, 1, 19)) as DATETIME) AS activity_end_date_time,
        substr(CASE
          WHEN upper(rtrim(dp.activitynote)) = '' THEN CAST(NULL as STRING)
          ELSE dp.activitynote
        END, 1, 255) AS activity_note_text,
        dp.checkedin AS check_in_ind,
        CAST(trim(substr(dp.patientarrivaldatetime, 1, 19)) as DATETIME) AS patient_arrival_date_time,
        CASE
           upper(rtrim(dp.waitlistedflag))
          WHEN '1' THEN 'Y'
          WHEN '0' THEN 'N'
        END AS waitlist_ind,
        substr(CASE
          WHEN upper(rtrim(dp.patientlocation)) = '' THEN CAST(NULL as STRING)
          ELSE dp.patientlocation
        END, 1, 100) AS patient_location_text,
        CASE
           upper(rtrim(dp.appointmentinstanceflag))
          WHEN '1' THEN 'Y'
          WHEN '0' THEN 'N'
        END AS appointment_instance_ind,
        CAST(trim(substr(dp.derivedappointmenttaskdate, 1, 19)) as DATETIME) AS appointment_task_date_time,
        CASE
           upper(rtrim(dp.activityownerflag))
          WHEN '1' THEN 'Y'
          WHEN '0' THEN 'N'
        END AS activity_owner_ind,
        CASE
           upper(rtrim(dp.isvisittypeopenchart))
          WHEN '1' THEN 'Y'
          WHEN '0' THEN 'N'
        END AS visit_type_open_chart_ind,
        CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dp.ctrresourceser) as INT64) AS resource_id,
        CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dp.logid) as INT64) AS log_id,
        CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dp.runid) as INT64) AS run_id,
        'R' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT
              stg_dimactivitytransaction.dimsiteid,
              stg_dimactivitytransaction.dimactivityid,
              stg_dimactivitytransaction.activitypriority,
              stg_dimactivitytransaction.dimhospitaldepartmentid,
              stg_dimactivitytransaction.dimpatientid,
              stg_dimactivitytransaction.dimlookupid_appointmentstatus,
              stg_dimactivitytransaction.dimlookupid_actualresourcetype,
              stg_dimactivitytransaction.dimlookupid_cancelreasontype,
              stg_dimactivitytransaction.dimlookupid_appointmentrsrcsta,
              stg_dimactivitytransaction.dimactivitytransactionid,
              stg_dimactivitytransaction.scheduledendtime,
              stg_dimactivitytransaction.appointmentdatetime,
              stg_dimactivitytransaction.isscheduled,
              stg_dimactivitytransaction.activitystartdatetime,
              stg_dimactivitytransaction.activityenddatetime,
              trim(stg_dimactivitytransaction.activitynote) AS activitynote,
              stg_dimactivitytransaction.checkedin,
              stg_dimactivitytransaction.patientarrivaldatetime,
              stg_dimactivitytransaction.waitlistedflag,
              trim(stg_dimactivitytransaction.patientlocation) AS patientlocation,
              stg_dimactivitytransaction.appointmentinstanceflag,
              stg_dimactivitytransaction.derivedappointmenttaskdate,
              stg_dimactivitytransaction.activityownerflag,
              stg_dimactivitytransaction.isvisittypeopenchart,
              stg_dimactivitytransaction.ctrresourceser,
              stg_dimactivitytransaction.logid,
              stg_dimactivitytransaction.runid
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimactivitytransaction
        ) AS dp
        INNER JOIN (
          SELECT
              ref_rad_onc_site.source_site_id,
              ref_rad_onc_site.site_sk
            FROM
              `hca-hin-dev-cur-ops`.edwcr_base_views.ref_rad_onc_site
        ) AS rr ON CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dp.dimsiteid) as FLOAT64) = rr.source_site_id
        LEFT OUTER JOIN (
          SELECT
              ref_rad_onc_activity_priority.activity_priority_sk,
              ref_rad_onc_activity_priority.activity_priority_desc
            FROM
              `hca-hin-dev-cur-ops`.edwcr_base_views.ref_rad_onc_activity_priority
        ) AS ap ON upper(rtrim(dp.activitypriority)) = upper(rtrim(ap.activity_priority_desc))
        LEFT OUTER JOIN (
          SELECT
              ref_rad_onc_hospital.source_hospital_id,
              ref_rad_onc_hospital.hospital_sk,
              ref_rad_onc_hospital.site_sk
            FROM
              `hca-hin-dev-cur-ops`.edwcr_base_views.ref_rad_onc_hospital
        ) AS rh ON CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dp.dimhospitaldepartmentid) as FLOAT64) = rh.source_hospital_id
         AND rr.site_sk = rh.site_sk
        LEFT OUTER JOIN (
          SELECT
              rad_onc_patient.source_patient_id,
              rad_onc_patient.patient_sk,
              rad_onc_patient.site_sk
            FROM
              `hca-hin-dev-cur-ops`.edwcr_base_views.rad_onc_patient
        ) AS rp ON CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dp.dimpatientid) as FLOAT64) = rp.source_patient_id
         AND rr.site_sk = rp.site_sk
        LEFT OUTER JOIN (
          SELECT
              ref_rad_onc_activity.source_activity_id,
              ref_rad_onc_activity.activity_sk,
              ref_rad_onc_activity.site_sk
            FROM
              `hca-hin-dev-cur-ops`.edwcr_base_views.ref_rad_onc_activity
        ) AS ra ON CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dp.dimactivityid) as FLOAT64) = ra.source_activity_id
         AND rr.site_sk = ra.site_sk
  ) AS ms
  ON mt.activity_transaction_sk = ms.fact_patient_toxicity_sk
   AND (coalesce(mt.activity_priority_sk, 0) = coalesce(ms.activity_priority_sk, 0)
   AND coalesce(mt.activity_priority_sk, 1) = coalesce(ms.activity_priority_sk, 1))
   AND (coalesce(mt.hospital_sk, 0) = coalesce(ms.hospital_sk, 0)
   AND coalesce(mt.hospital_sk, 1) = coalesce(ms.hospital_sk, 1))
   AND (coalesce(mt.patient_sk, 0) = coalesce(ms.patient_sk, 0)
   AND coalesce(mt.patient_sk, 1) = coalesce(ms.patient_sk, 1))
   AND (coalesce(mt.activity_sk, 0) = coalesce(ms.activity_sk, 0)
   AND coalesce(mt.activity_sk, 1) = coalesce(ms.activity_sk, 1))
   AND (coalesce(mt.appointment_status_id, 0) = coalesce(ms.appointment_status_id, 0)
   AND coalesce(mt.appointment_status_id, 1) = coalesce(ms.appointment_status_id, 1))
   AND (coalesce(mt.actual_resource_type_id, 0) = coalesce(ms.actual_resource_type_id, 0)
   AND coalesce(mt.actual_resource_type_id, 1) = coalesce(ms.actual_resource_type_id, 1))
   AND (coalesce(mt.cancel_reason_type_id, 0) = coalesce(ms.cancel_reason_type_id, 0)
   AND coalesce(mt.cancel_reason_type_id, 1) = coalesce(ms.cancel_reason_type_id, 1))
   AND (coalesce(mt.appointment_resource_status_id, 0) = coalesce(ms.appointment_resource_status_id, 0)
   AND coalesce(mt.appointment_resource_status_id, 1) = coalesce(ms.appointment_resource_status_id, 1))
   AND mt.site_sk = ms.site_sk
   AND mt.source_activity_transaction_id = ms.source_activity_transaction_id
   AND (coalesce(mt.schedule_end_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.schedule_end_date_time, DATETIME '1970-01-01 00:00:00')
   AND coalesce(mt.schedule_end_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.schedule_end_date_time, DATETIME '1970-01-01 00:00:01'))
   AND (coalesce(mt.appointment_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.appointment_date_time, DATETIME '1970-01-01 00:00:00')
   AND coalesce(mt.appointment_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.appointment_date_time, DATETIME '1970-01-01 00:00:01'))
   AND (upper(coalesce(mt.appointment_schedule_ind, '0')) = upper(coalesce(ms.appointment_schedule_ind, '0'))
   AND upper(coalesce(mt.appointment_schedule_ind, '1')) = upper(coalesce(ms.appointment_schedule_ind, '1')))
   AND (coalesce(mt.activity_start_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.activity_start_date_time, DATETIME '1970-01-01 00:00:00')
   AND coalesce(mt.activity_start_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.activity_start_date_time, DATETIME '1970-01-01 00:00:01'))
   AND (coalesce(mt.activity_end_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.activity_end_date_time, DATETIME '1970-01-01 00:00:00')
   AND coalesce(mt.activity_end_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.activity_end_date_time, DATETIME '1970-01-01 00:00:01'))
   AND (upper(coalesce(mt.activity_note_text, '0')) = upper(coalesce(ms.activity_note_text, '0'))
   AND upper(coalesce(mt.activity_note_text, '1')) = upper(coalesce(ms.activity_note_text, '1')))
   AND (upper(coalesce(mt.check_in_ind, '0')) = upper(coalesce(ms.check_in_ind, '0'))
   AND upper(coalesce(mt.check_in_ind, '1')) = upper(coalesce(ms.check_in_ind, '1')))
   AND (coalesce(mt.patient_arrival_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.patient_arrival_date_time, DATETIME '1970-01-01 00:00:00')
   AND coalesce(mt.patient_arrival_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.patient_arrival_date_time, DATETIME '1970-01-01 00:00:01'))
   AND (upper(coalesce(mt.waitlist_ind, '0')) = upper(coalesce(ms.waitlist_ind, '0'))
   AND upper(coalesce(mt.waitlist_ind, '1')) = upper(coalesce(ms.waitlist_ind, '1')))
   AND (upper(coalesce(mt.patient_location_text, '0')) = upper(coalesce(ms.patient_location_text, '0'))
   AND upper(coalesce(mt.patient_location_text, '1')) = upper(coalesce(ms.patient_location_text, '1')))
   AND (upper(coalesce(mt.appointment_instance_ind, '0')) = upper(coalesce(ms.appointment_instance_ind, '0'))
   AND upper(coalesce(mt.appointment_instance_ind, '1')) = upper(coalesce(ms.appointment_instance_ind, '1')))
   AND (coalesce(mt.appointment_task_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.appointment_task_date_time, DATETIME '1970-01-01 00:00:00')
   AND coalesce(mt.appointment_task_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.appointment_task_date_time, DATETIME '1970-01-01 00:00:01'))
   AND (upper(coalesce(mt.activity_owner_ind, '0')) = upper(coalesce(ms.activity_owner_ind, '0'))
   AND upper(coalesce(mt.activity_owner_ind, '1')) = upper(coalesce(ms.activity_owner_ind, '1')))
   AND (upper(coalesce(mt.visit_type_open_chart_ind, '0')) = upper(coalesce(ms.visit_type_open_chart_ind, '0'))
   AND upper(coalesce(mt.visit_type_open_chart_ind, '1')) = upper(coalesce(ms.visit_type_open_chart_ind, '1')))
   AND (coalesce(mt.resource_id, 0) = coalesce(ms.resource_id, 0)
   AND coalesce(mt.resource_id, 1) = coalesce(ms.resource_id, 1))
   AND (coalesce(mt.log_id, 0) = coalesce(ms.log_id, 0)
   AND coalesce(mt.log_id, 1) = coalesce(ms.log_id, 1))
   AND (coalesce(mt.run_id, 0) = coalesce(ms.run_id, 0)
   AND coalesce(mt.run_id, 1) = coalesce(ms.run_id, 1))
   AND mt.source_system_code = ms.source_system_code
   AND mt.dw_last_update_date_time = ms.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN
      INSERT (activity_transaction_sk, activity_priority_sk, hospital_sk, patient_sk, activity_sk, appointment_status_id, actual_resource_type_id, cancel_reason_type_id, appointment_resource_status_id, site_sk, source_activity_transaction_id, schedule_end_date_time, appointment_date_time, appointment_schedule_ind, activity_start_date_time, activity_end_date_time, activity_note_text, check_in_ind, patient_arrival_date_time, waitlist_ind, patient_location_text, appointment_instance_ind, appointment_task_date_time, activity_owner_ind, visit_type_open_chart_ind, resource_id, log_id, run_id, source_system_code, dw_last_update_date_time)
      VALUES (ms.fact_patient_toxicity_sk, ms.activity_priority_sk, ms.hospital_sk, ms.patient_sk, ms.activity_sk, ms.appointment_status_id, ms.actual_resource_type_id, ms.cancel_reason_type_id, ms.appointment_resource_status_id, ms.site_sk, ms.source_activity_transaction_id, ms.schedule_end_date_time, ms.appointment_date_time, ms.appointment_schedule_ind, ms.activity_start_date_time, ms.activity_end_date_time, ms.activity_note_text, ms.check_in_ind, ms.patient_arrival_date_time, ms.waitlist_ind, ms.patient_location_text, ms.appointment_instance_ind, ms.appointment_task_date_time, ms.activity_owner_ind, ms.visit_type_open_chart_ind, ms.resource_id, ms.log_id, ms.run_id, ms.source_system_code, ms.dw_last_update_date_time)
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','Rad_Onc_Activity_Transaction');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- EOF
