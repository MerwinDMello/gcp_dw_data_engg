DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/fact_rad_onc_activity_billing.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.Fact_Rad_Onc_Activity_Billing                        	##
-- ##  TARGET  DATABASE	   : EDWCR	 											##
-- ##  SOURCE		  		   : EDWCR_Staging.FACTACTIVITYBILLING_STG 						##
-- ##														  						##
-- ##	                                                                        	##
-- ##  INITIAL RELEASE	   : 														##
-- ##  PROJECT            	   : 	 		    									##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_RO_Fact_Rad_Onc_Activity_Billing;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','FACTACTIVITYBILLING_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- Deleting Data
BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.fact_rad_onc_activity_billing;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.fact_rad_onc_activity_billing AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY dp.dimsiteid,
                                               dp.factactivitybillingid DESC) AS fact_activity_billing_sk,
                                     rop.physician_sk AS physician_sk,
                                     rop1.physician_sk AS attending_oncologist_id,
                                     pc.patient_course_sk AS patient_course_sk,
                                     rh.hospital_sk AS hospital_sk,
                                     ra.activity_sk AS activity_sk,
                                     rat.activity_transaction_sk AS activity_transaction_sk,
                                     rpp.procedure_code_sk AS procedure_code_sk,
                                     rp.patient_sk AS patient_sk,
                                     CAST(dp.dimlkpid_activitycategory AS INT64) AS activity_category_id,
                                     rr.site_sk AS site_sk,
                                     CAST(dp.factactivitybillingid AS INT64) AS source_fact_activity_billing_id,
                                     ROUND(dp.primaryglobalcharge, 6, 'ROUND_HALF_EVEN') AS primary_global_charge_amt,
                                     ROUND(dp.primarytechnicalcharge, 6, 'ROUND_HALF_EVEN') AS primary_technical_charge_amt,
                                     ROUND(dp.primaryprofessionalcharge, 6, 'ROUND_HALF_EVEN') AS primary_professional_charge_amt,
                                     ROUND(dp.otherprofessionalcharge, 6, 'ROUND_HALF_EVEN') AS other_professional_charge_amt,
                                     ROUND(dp.othertechnicalcharge, 6, 'ROUND_HALF_EVEN') AS other_technical_charge_amt,
                                     ROUND(dp.otherglobalcharge, 6, 'ROUND_HALF_EVEN') AS other_global_charge_amt,
                                     ROUND(dp.chargeforecast, 6, 'ROUND_HALF_EVEN') AS forecast_charge_amt,
                                     ROUND(dp.actualcharge, 6, 'ROUND_HALF_EVEN') AS actual_charge_amt,
                                     ROUND(dp.activitycost, 6, 'ROUND_HALF_EVEN') AS activity_cost_amt,
                                     substr(CASE
                                                WHEN upper(rtrim(dp.accountbillingcode)) = '' THEN CAST(NULL AS STRING)
                                                ELSE dp.accountbillingcode
                                            END, 1, 20) AS activity_billing_code_text,
                                     CAST(trim(dp.fromdateofservice) AS DATETIME) AS service_start_date_time,
                                     CAST(trim(dp.todateofservice) AS DATETIME) AS service_end_date_time,
                                     CAST(trim(dp.completeddatetime) AS DATETIME) AS complete_date_time,
                                     CAST(trim(dp.exporteddatetime) AS DATETIME) AS export_date_time,
                                     CAST(trim(dp.markedcompleteddatetime) AS DATETIME) AS mark_complete_date_time,
                                     CAST(trim(dp.creditexporteddatetime) AS DATETIME) AS credit_export_date_time,
                                     CAST(trim(dp.crediteddatetime) AS DATETIME) AS credit_date_time,
                                     substr(CASE
                                                WHEN upper(rtrim(dp.creditnote)) = '' THEN CAST(NULL AS STRING)
                                                ELSE dp.creditnote
                                            END, 1, 255) AS credit_note_text,
                                     substr(CASE
                                                WHEN upper(rtrim(dp.allmodifiercodes)) = '' THEN CAST(NULL AS STRING)
                                                ELSE dp.allmodifiercodes
                                            END, 1, 100) AS modifier_code_text,
                                     ROUND(CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dp.creditamount) AS NUMERIC), 6, 'ROUND_HALF_EVEN') AS credit_amt,
                                     dp.isscheduled AS scheduled_ind,
                                     CASE upper(rtrim(dp.objectstatus))
                                         WHEN 'ACTIVE' THEN 'Y'
                                         WHEN 'DELETED' THEN 'N'
                                         ELSE 'U'
                                     END AS object_status_ind,
                                     CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dp.logid) AS INT64) AS log_id,
                                     CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dp.runid) AS INT64) AS run_id,
                                     'R' AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT factactivitybilling_stg.dimdoctorid,
             factactivitybilling_stg.dimdoctid_attdoncologist,
             factactivitybilling_stg.dimcourseid,
             factactivitybilling_stg.dimhospitaldepartmentid,
             factactivitybilling_stg.dimactivityid,
             factactivitybilling_stg.dimactivitytransactionid,
             factactivitybilling_stg.dimprocedurecodeid,
             factactivitybilling_stg.dimpatientid,
             factactivitybilling_stg.dimlkpid_activitycategory,
             factactivitybilling_stg.dimsiteid,
             factactivitybilling_stg.factactivitybillingid,
             factactivitybilling_stg.primaryglobalcharge,
             factactivitybilling_stg.primarytechnicalcharge,
             factactivitybilling_stg.primaryprofessionalcharge,
             factactivitybilling_stg.otherprofessionalcharge,
             factactivitybilling_stg.othertechnicalcharge,
             factactivitybilling_stg.otherglobalcharge,
             factactivitybilling_stg.chargeforecast,
             factactivitybilling_stg.actualcharge,
             factactivitybilling_stg.activitycost,
             trim(factactivitybilling_stg.accountbillingcode) AS accountbillingcode,
             factactivitybilling_stg.fromdateofservice,
             factactivitybilling_stg.todateofservice,
             factactivitybilling_stg.completeddatetime,
             factactivitybilling_stg.exporteddatetime,
             factactivitybilling_stg.markedcompleteddatetime,
             factactivitybilling_stg.creditexporteddatetime,
             factactivitybilling_stg.crediteddatetime,
             trim(factactivitybilling_stg.creditnote) AS creditnote,
             trim(factactivitybilling_stg.allmodifiercodes) AS allmodifiercodes,
             factactivitybilling_stg.creditamount,
             factactivitybilling_stg.isscheduled,
             factactivitybilling_stg.objectstatus,
             factactivitybilling_stg.logid,
             factactivitybilling_stg.runid
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.factactivitybilling_stg) AS dp
   INNER JOIN
     (SELECT ref_rad_onc_site.source_site_id,
             ref_rad_onc_site.site_sk
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.ref_rad_onc_site) AS rr ON dp.dimsiteid = rr.source_site_id
   LEFT OUTER JOIN
     (SELECT rad_onc_physician.source_physician_id,
             rad_onc_physician.physician_sk,
             rad_onc_physician.site_sk
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.rad_onc_physician) AS rop ON dp.dimdoctorid = rop.source_physician_id
   AND rr.site_sk = rop.site_sk
   LEFT OUTER JOIN
     (SELECT rad_onc_physician.source_physician_id,
             rad_onc_physician.physician_sk,
             rad_onc_physician.site_sk
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.rad_onc_physician) AS rop1 ON dp.dimdoctid_attdoncologist = rop1.source_physician_id
   AND rr.site_sk = rop1.site_sk
   LEFT OUTER JOIN
     (SELECT rad_onc_patient_course.source_patient_course_id,
             rad_onc_patient_course.patient_course_sk,
             rad_onc_patient_course.site_sk
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.rad_onc_patient_course) AS pc ON dp.dimcourseid = pc.source_patient_course_id
   AND rr.site_sk = pc.site_sk
   LEFT OUTER JOIN
     (SELECT ref_rad_onc_hospital.source_hospital_id,
             ref_rad_onc_hospital.hospital_sk,
             ref_rad_onc_hospital.site_sk
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.ref_rad_onc_hospital) AS rh ON dp.dimhospitaldepartmentid = rh.source_hospital_id
   AND rr.site_sk = rh.site_sk
   LEFT OUTER JOIN
     (SELECT ref_rad_onc_activity.source_activity_id,
             ref_rad_onc_activity.activity_sk,
             ref_rad_onc_activity.site_sk
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.ref_rad_onc_activity) AS ra ON dp.dimactivityid = ra.source_activity_id
   AND rr.site_sk = ra.site_sk
   LEFT OUTER JOIN
     (SELECT rad_onc_activity_transaction.source_activity_transaction_id,
             rad_onc_activity_transaction.activity_transaction_sk,
             rad_onc_activity_transaction.site_sk
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.rad_onc_activity_transaction) AS rat ON dp.dimactivitytransactionid = rat.source_activity_transaction_id
   AND rr.site_sk = rat.site_sk
   LEFT OUTER JOIN
     (SELECT ref_rad_onc_procedure_code.source_procedure_code_id,
             ref_rad_onc_procedure_code.procedure_code_sk,
             ref_rad_onc_procedure_code.site_sk
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.ref_rad_onc_procedure_code) AS rpp ON dp.dimprocedurecodeid = rpp.source_procedure_code_id
   AND rr.site_sk = rpp.site_sk
   LEFT OUTER JOIN
     (SELECT rad_onc_patient.source_patient_id,
             rad_onc_patient.patient_sk,
             rad_onc_patient.site_sk
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.rad_onc_patient) AS rp ON dp.dimpatientid = rp.source_patient_id
   AND rr.site_sk = rp.site_sk) AS ms ON mt.fact_activity_billing_sk = ms.fact_activity_billing_sk
AND (coalesce(mt.physician_sk, 0) = coalesce(ms.physician_sk, 0)
     AND coalesce(mt.physician_sk, 1) = coalesce(ms.physician_sk, 1))
AND (coalesce(mt.attending_oncologist_id, 0) = coalesce(ms.attending_oncologist_id, 0)
     AND coalesce(mt.attending_oncologist_id, 1) = coalesce(ms.attending_oncologist_id, 1))
AND (coalesce(mt.patient_course_sk, 0) = coalesce(ms.patient_course_sk, 0)
     AND coalesce(mt.patient_course_sk, 1) = coalesce(ms.patient_course_sk, 1))
AND (coalesce(mt.hospital_sk, 0) = coalesce(ms.hospital_sk, 0)
     AND coalesce(mt.hospital_sk, 1) = coalesce(ms.hospital_sk, 1))
AND (coalesce(mt.activity_sk, 0) = coalesce(ms.activity_sk, 0)
     AND coalesce(mt.activity_sk, 1) = coalesce(ms.activity_sk, 1))
AND (coalesce(mt.activity_transaction_sk, 0) = coalesce(ms.activity_transaction_sk, 0)
     AND coalesce(mt.activity_transaction_sk, 1) = coalesce(ms.activity_transaction_sk, 1))
AND (coalesce(mt.procedure_code_sk, 0) = coalesce(ms.procedure_code_sk, 0)
     AND coalesce(mt.procedure_code_sk, 1) = coalesce(ms.procedure_code_sk, 1))
AND (coalesce(mt.patient_sk, 0) = coalesce(ms.patient_sk, 0)
     AND coalesce(mt.patient_sk, 1) = coalesce(ms.patient_sk, 1))
AND (coalesce(mt.activity_category_id, 0) = coalesce(ms.activity_category_id, 0)
     AND coalesce(mt.activity_category_id, 1) = coalesce(ms.activity_category_id, 1))
AND mt.site_sk = ms.site_sk
AND mt.source_fact_activity_billing_id = ms.source_fact_activity_billing_id
AND (coalesce(mt.primary_global_charge_amt, NUMERIC '0') = coalesce(ms.primary_global_charge_amt, NUMERIC '0')
     AND coalesce(mt.primary_global_charge_amt, NUMERIC '1') = coalesce(ms.primary_global_charge_amt, NUMERIC '1'))
AND (coalesce(mt.primary_technical_charge_amt, NUMERIC '0') = coalesce(ms.primary_technical_charge_amt, NUMERIC '0')
     AND coalesce(mt.primary_technical_charge_amt, NUMERIC '1') = coalesce(ms.primary_technical_charge_amt, NUMERIC '1'))
AND (coalesce(mt.primary_professional_charge_amt, NUMERIC '0') = coalesce(ms.primary_professional_charge_amt, NUMERIC '0')
     AND coalesce(mt.primary_professional_charge_amt, NUMERIC '1') = coalesce(ms.primary_professional_charge_amt, NUMERIC '1'))
AND (coalesce(mt.other_professional_charge_amt, NUMERIC '0') = coalesce(ms.other_professional_charge_amt, NUMERIC '0')
     AND coalesce(mt.other_professional_charge_amt, NUMERIC '1') = coalesce(ms.other_professional_charge_amt, NUMERIC '1'))
AND (coalesce(mt.other_technical_charge_amt, NUMERIC '0') = coalesce(ms.other_technical_charge_amt, NUMERIC '0')
     AND coalesce(mt.other_technical_charge_amt, NUMERIC '1') = coalesce(ms.other_technical_charge_amt, NUMERIC '1'))
AND (coalesce(mt.other_global_charge_amt, NUMERIC '0') = coalesce(ms.other_global_charge_amt, NUMERIC '0')
     AND coalesce(mt.other_global_charge_amt, NUMERIC '1') = coalesce(ms.other_global_charge_amt, NUMERIC '1'))
AND (coalesce(mt.forecast_charge_amt, NUMERIC '0') = coalesce(ms.forecast_charge_amt, NUMERIC '0')
     AND coalesce(mt.forecast_charge_amt, NUMERIC '1') = coalesce(ms.forecast_charge_amt, NUMERIC '1'))
AND (coalesce(mt.actual_charge_amt, NUMERIC '0') = coalesce(ms.actual_charge_amt, NUMERIC '0')
     AND coalesce(mt.actual_charge_amt, NUMERIC '1') = coalesce(ms.actual_charge_amt, NUMERIC '1'))
AND (coalesce(mt.activity_cost_amt, NUMERIC '0') = coalesce(ms.activity_cost_amt, NUMERIC '0')
     AND coalesce(mt.activity_cost_amt, NUMERIC '1') = coalesce(ms.activity_cost_amt, NUMERIC '1'))
AND (upper(coalesce(mt.activity_billing_code_text, '0')) = upper(coalesce(ms.activity_billing_code_text, '0'))
     AND upper(coalesce(mt.activity_billing_code_text, '1')) = upper(coalesce(ms.activity_billing_code_text, '1')))
AND (coalesce(mt.service_start_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.service_start_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.service_start_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.service_start_date_time, DATETIME '1970-01-01 00:00:01'))
AND (coalesce(mt.service_end_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.service_end_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.service_end_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.service_end_date_time, DATETIME '1970-01-01 00:00:01'))
AND (coalesce(mt.complete_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.complete_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.complete_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.complete_date_time, DATETIME '1970-01-01 00:00:01'))
AND (coalesce(mt.export_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.export_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.export_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.export_date_time, DATETIME '1970-01-01 00:00:01'))
AND (coalesce(mt.mark_complete_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.mark_complete_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.mark_complete_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.mark_complete_date_time, DATETIME '1970-01-01 00:00:01'))
AND (coalesce(mt.credit_export_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.credit_export_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.credit_export_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.credit_export_date_time, DATETIME '1970-01-01 00:00:01'))
AND (coalesce(mt.credit_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.credit_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.credit_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.credit_date_time, DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.credit_note_text, '0')) = upper(coalesce(ms.credit_note_text, '0'))
     AND upper(coalesce(mt.credit_note_text, '1')) = upper(coalesce(ms.credit_note_text, '1')))
AND (upper(coalesce(mt.modifier_code_text, '0')) = upper(coalesce(ms.modifier_code_text, '0'))
     AND upper(coalesce(mt.modifier_code_text, '1')) = upper(coalesce(ms.modifier_code_text, '1')))
AND (coalesce(mt.credit_amt, NUMERIC '0') = coalesce(ms.credit_amt, NUMERIC '0')
     AND coalesce(mt.credit_amt, NUMERIC '1') = coalesce(ms.credit_amt, NUMERIC '1'))
AND (upper(coalesce(mt.scheduled_ind, '0')) = upper(coalesce(ms.scheduled_ind, '0'))
     AND upper(coalesce(mt.scheduled_ind, '1')) = upper(coalesce(ms.scheduled_ind, '1')))
AND (upper(coalesce(mt.object_status_ind, '0')) = upper(coalesce(ms.object_status_ind, '0'))
     AND upper(coalesce(mt.object_status_ind, '1')) = upper(coalesce(ms.object_status_ind, '1')))
AND (coalesce(mt.log_id, 0) = coalesce(ms.log_id, 0)
     AND coalesce(mt.log_id, 1) = coalesce(ms.log_id, 1))
AND (coalesce(mt.run_id, 0) = coalesce(ms.run_id, 0)
     AND coalesce(mt.run_id, 1) = coalesce(ms.run_id, 1))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (fact_activity_billing_sk,
        physician_sk,
        attending_oncologist_id,
        patient_course_sk,
        hospital_sk,
        activity_sk,
        activity_transaction_sk,
        procedure_code_sk,
        patient_sk,
        activity_category_id,
        site_sk,
        source_fact_activity_billing_id,
        primary_global_charge_amt,
        primary_technical_charge_amt,
        primary_professional_charge_amt,
        other_professional_charge_amt,
        other_technical_charge_amt,
        other_global_charge_amt,
        forecast_charge_amt,
        actual_charge_amt,
        activity_cost_amt,
        activity_billing_code_text,
        service_start_date_time,
        service_end_date_time,
        complete_date_time,
        export_date_time,
        mark_complete_date_time,
        credit_export_date_time,
        credit_date_time,
        credit_note_text,
        modifier_code_text,
        credit_amt,
        scheduled_ind,
        object_status_ind,
        log_id,
        run_id,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.fact_activity_billing_sk, ms.physician_sk, ms.attending_oncologist_id, ms.patient_course_sk, ms.hospital_sk, ms.activity_sk, ms.activity_transaction_sk, ms.procedure_code_sk, ms.patient_sk, ms.activity_category_id, ms.site_sk, ms.source_fact_activity_billing_id, ms.primary_global_charge_amt, ms.primary_technical_charge_amt, ms.primary_professional_charge_amt, ms.other_professional_charge_amt, ms.other_technical_charge_amt, ms.other_global_charge_amt, ms.forecast_charge_amt, ms.actual_charge_amt, ms.activity_cost_amt, ms.activity_billing_code_text, ms.service_start_date_time, ms.service_end_date_time, ms.complete_date_time, ms.export_date_time, ms.mark_complete_date_time, ms.credit_export_date_time, ms.credit_date_time, ms.credit_note_text, ms.modifier_code_text, ms.credit_amt, ms.scheduled_ind, ms.object_status_ind, ms.log_id, ms.run_id, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT fact_activity_billing_sk
      FROM `hca-hin-dev-cur-ops`.edwcr.fact_rad_onc_activity_billing
      GROUP BY fact_activity_billing_sk
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.fact_rad_onc_activity_billing');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','Fact_Rad_Onc_Activity_Billing');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF;;