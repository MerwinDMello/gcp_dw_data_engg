DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/fact_rad_onc_treatment_history.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.Fact_rad_Onc_Treatment_History                   ##
-- ##  TARGET  DATABASE	   : EDWCR	 											##
-- ##  SOURCE		  		   : EDWCR_Staging.FactTreatmentHist_STG 				##
-- ##														  						##
-- ##	                                                                        	##
-- ##  INITIAL RELEASE	   : 														##
-- ##  PROJECT            	   : 	 		    									##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_RO_Fact_rad_Onc_Treatment_History;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_Staging','FactTreatmentHist_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- Deleting Data
BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.fact_rad_onc_treatment_history;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.fact_rad_onc_treatment_history AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY CAST(dp.dimsiteid AS INT64),
                                               CAST(dp.facttreatmenthistoryid AS INT64)) AS fact_treatment_history_sk,
                                     ra.patient_course_sk AS patient_course_sk,
                                     pp.patient_plan_sk AS patient_plan_sk,
                                     ra1.patient_sk AS patient_sk,
                                     CAST(dp.dimlkpid_treatmentintenttype AS INT64) AS treatment_intent_type_id,
                                     CAST(dp.dimlookupid_clinicalstatus AS INT64) AS clinical_status_id,
                                     CAST(dp.dimlookupid_planstatus AS INT64) AS plan_status_id,
                                     CAST(dp.dimlookupid_fieldtechnique AS INT64) AS field_technique_id,
                                     CAST(dp.dimlookupid_technique AS INT64) AS technique_id,
                                     CAST(dp.dimlookupid_techniquelabel AS INT64) AS technique_label_id,
                                     CAST(dp.dimlkpid_treatmentdeliverytyp AS INT64) AS technique_delivery_type_id,
                                     rr.site_sk AS site_sk,
                                     CAST(dp.facttreatmenthistoryid AS INT64) AS source_fact_treatment_history_id,
                                     dp.completion_date_time,
                                     dp.first_treatment_date_time,
                                     dp.last_treatment_date_time,
                                     dp.status_date_time,
                                     dp.active_ind AS active_ind,
                                     CAST(dp.planneddoserate AS INT64) AS planned_dose_rate_num,
                                     ROUND(CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dp.coursedosedelivered) AS NUMERIC), 38, 'ROUND_HALF_EVEN') AS course_dose_delivered_amt,
                                     ROUND(CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dp.coursedoseplanned) AS NUMERIC), 38, 'ROUND_HALF_EVEN') AS course_dose_planned_amt,
                                     ROUND(CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dp.coursedoseremaining) AS NUMERIC), 38, 'ROUND_HALF_EVEN') AS course_dose_remaining_amt,
                                     ROUND(CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dp.othercoursedosedelivered) AS NUMERIC), 38, 'ROUND_HALF_EVEN') AS other_course_dose_delivered_amt,
                                     ROUND(CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dp.dosecorrection) AS NUMERIC), 38, 'ROUND_HALF_EVEN') AS dose_correction_amt,
                                     ROUND(CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dp.totaldoselimit) AS NUMERIC), 38, 'ROUND_HALF_EVEN') AS total_dose_limit_amt,
                                     ROUND(CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dp.dailydoselimit) AS NUMERIC), 38, 'ROUND_HALF_EVEN') AS daily_dose_limit_amt,
                                     ROUND(CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dp.sessiondoselimit) AS NUMERIC), 38, 'ROUND_HALF_EVEN') AS session_dose_limit_amt,
                                     dp.primary_ind AS primary_ind,
                                     CAST(dp.logid AS INT64) AS log_id,
                                     CAST(dp.runid AS INT64) AS run_id,
                                     'R' AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT facttreatmenthist_stg.dimsiteid,
             facttreatmenthist_stg.dimcourseid,
             facttreatmenthist_stg.dimplanid,
             facttreatmenthist_stg.dimpatientid,
             facttreatmenthist_stg.dimlkpid_treatmentintenttype,
             facttreatmenthist_stg.dimlookupid_clinicalstatus,
             facttreatmenthist_stg.dimlookupid_planstatus,
             facttreatmenthist_stg.dimlookupid_fieldtechnique,
             facttreatmenthist_stg.dimlookupid_technique,
             facttreatmenthist_stg.dimlookupid_techniquelabel,
             facttreatmenthist_stg.dimlkpid_treatmentdeliverytyp,
             facttreatmenthist_stg.facttreatmenthistoryid,
             CAST(trim(facttreatmenthist_stg.completeddatetime) AS DATETIME) AS completion_date_time,
             CAST(trim(facttreatmenthist_stg.firsttreatmentdate) AS DATETIME) AS first_treatment_date_time,
             CAST(trim(facttreatmenthist_stg.lasttreatmentdate) AS DATETIME) AS last_treatment_date_time,
             CAST(trim(facttreatmenthist_stg.statusdate) AS DATETIME) AS status_date_time,
             CASE facttreatmenthist_stg.isactive
                 WHEN 1 THEN 'Y'
                 WHEN 0 THEN 'N'
             END AS active_ind,
             facttreatmenthist_stg.planneddoserate,
             trim(facttreatmenthist_stg.coursedosedelivered) AS coursedosedelivered,
             trim(facttreatmenthist_stg.coursedoseplanned) AS coursedoseplanned,
             trim(facttreatmenthist_stg.coursedoseremaining) AS coursedoseremaining,
             trim(facttreatmenthist_stg.othercoursedosedelivered) AS othercoursedosedelivered,
             trim(facttreatmenthist_stg.dosecorrection) AS dosecorrection,
             trim(facttreatmenthist_stg.totaldoselimit) AS totaldoselimit,
             trim(facttreatmenthist_stg.dailydoselimit) AS dailydoselimit,
             trim(facttreatmenthist_stg.sessiondoselimit) AS sessiondoselimit,
             CASE facttreatmenthist_stg.primaryflag
                 WHEN 1 THEN 'Y'
                 WHEN 0 THEN 'N'
             END AS primary_ind,
             facttreatmenthist_stg.logid,
             facttreatmenthist_stg.runid
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.facttreatmenthist_stg) AS dp
   INNER JOIN
     (SELECT ref_rad_onc_site.source_site_id,
             ref_rad_onc_site.site_sk
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.ref_rad_onc_site) AS rr ON rr.source_site_id = dp.dimsiteid
   LEFT OUTER JOIN
     (SELECT rad_onc_patient_course.patient_course_sk AS patient_course_sk,
             rad_onc_patient_course.source_patient_course_id,
             rad_onc_patient_course.site_sk
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.rad_onc_patient_course) AS ra ON dp.dimcourseid = ra.source_patient_course_id
   AND rr.site_sk = ra.site_sk
   LEFT OUTER JOIN
     (SELECT rad_onc_patient_plan.source_patient_plan_id,
             rad_onc_patient_plan.site_sk,
             rad_onc_patient_plan.patient_plan_sk
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.rad_onc_patient_plan) AS pp ON dp.dimplanid = pp.source_patient_plan_id
   AND rr.site_sk = pp.site_sk
   LEFT OUTER JOIN
     (SELECT rad_onc_patient.source_patient_id,
             rad_onc_patient.site_sk,
             rad_onc_patient.patient_sk
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.rad_onc_patient) AS ra1 ON dp.dimpatientid = ra1.source_patient_id
   AND rr.site_sk = ra1.site_sk) AS ms ON mt.fact_treatment_history_sk = ms.fact_treatment_history_sk
AND (coalesce(mt.patient_course_sk, 0) = coalesce(ms.patient_course_sk, 0)
     AND coalesce(mt.patient_course_sk, 1) = coalesce(ms.patient_course_sk, 1))
AND (coalesce(mt.patient_plan_sk, 0) = coalesce(ms.patient_plan_sk, 0)
     AND coalesce(mt.patient_plan_sk, 1) = coalesce(ms.patient_plan_sk, 1))
AND (coalesce(mt.patient_sk, 0) = coalesce(ms.patient_sk, 0)
     AND coalesce(mt.patient_sk, 1) = coalesce(ms.patient_sk, 1))
AND (coalesce(mt.treatment_intent_type_id, 0) = coalesce(ms.treatment_intent_type_id, 0)
     AND coalesce(mt.treatment_intent_type_id, 1) = coalesce(ms.treatment_intent_type_id, 1))
AND (coalesce(mt.clinical_status_id, 0) = coalesce(ms.clinical_status_id, 0)
     AND coalesce(mt.clinical_status_id, 1) = coalesce(ms.clinical_status_id, 1))
AND (coalesce(mt.plan_status_id, 0) = coalesce(ms.plan_status_id, 0)
     AND coalesce(mt.plan_status_id, 1) = coalesce(ms.plan_status_id, 1))
AND (coalesce(mt.field_technique_id, 0) = coalesce(ms.field_technique_id, 0)
     AND coalesce(mt.field_technique_id, 1) = coalesce(ms.field_technique_id, 1))
AND (coalesce(mt.technique_id, 0) = coalesce(ms.technique_id, 0)
     AND coalesce(mt.technique_id, 1) = coalesce(ms.technique_id, 1))
AND (coalesce(mt.technique_label_id, 0) = coalesce(ms.technique_label_id, 0)
     AND coalesce(mt.technique_label_id, 1) = coalesce(ms.technique_label_id, 1))
AND (coalesce(mt.technique_delivery_type_id, 0) = coalesce(ms.technique_delivery_type_id, 0)
     AND coalesce(mt.technique_delivery_type_id, 1) = coalesce(ms.technique_delivery_type_id, 1))
AND mt.site_sk = ms.site_sk
AND mt.source_fact_treatment_history_id = ms.source_fact_treatment_history_id
AND (coalesce(mt.completion_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.completion_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.completion_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.completion_date_time, DATETIME '1970-01-01 00:00:01'))
AND (coalesce(mt.first_treatment_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.first_treatment_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.first_treatment_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.first_treatment_date_time, DATETIME '1970-01-01 00:00:01'))
AND (coalesce(mt.last_treatment_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.last_treatment_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.last_treatment_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.last_treatment_date_time, DATETIME '1970-01-01 00:00:01'))
AND (coalesce(mt.status_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.status_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.status_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.status_date_time, DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.active_ind, '0')) = upper(coalesce(ms.active_ind, '0'))
     AND upper(coalesce(mt.active_ind, '1')) = upper(coalesce(ms.active_ind, '1')))
AND (coalesce(mt.planned_dose_rate_num, 0) = coalesce(ms.planned_dose_rate_num, 0)
     AND coalesce(mt.planned_dose_rate_num, 1) = coalesce(ms.planned_dose_rate_num, 1))
AND (coalesce(mt.course_dose_delivered_amt, NUMERIC '0') = coalesce(ms.course_dose_delivered_amt, NUMERIC '0')
     AND coalesce(mt.course_dose_delivered_amt, NUMERIC '1') = coalesce(ms.course_dose_delivered_amt, NUMERIC '1'))
AND (coalesce(mt.course_dose_planned_amt, NUMERIC '0') = coalesce(ms.course_dose_planned_amt, NUMERIC '0')
     AND coalesce(mt.course_dose_planned_amt, NUMERIC '1') = coalesce(ms.course_dose_planned_amt, NUMERIC '1'))
AND (coalesce(mt.course_dose_remaining_amt, NUMERIC '0') = coalesce(ms.course_dose_remaining_amt, NUMERIC '0')
     AND coalesce(mt.course_dose_remaining_amt, NUMERIC '1') = coalesce(ms.course_dose_remaining_amt, NUMERIC '1'))
AND (coalesce(mt.other_course_dose_delivered_amt, NUMERIC '0') = coalesce(ms.other_course_dose_delivered_amt, NUMERIC '0')
     AND coalesce(mt.other_course_dose_delivered_amt, NUMERIC '1') = coalesce(ms.other_course_dose_delivered_amt, NUMERIC '1'))
AND (coalesce(mt.dose_correction_amt, NUMERIC '0') = coalesce(ms.dose_correction_amt, NUMERIC '0')
     AND coalesce(mt.dose_correction_amt, NUMERIC '1') = coalesce(ms.dose_correction_amt, NUMERIC '1'))
AND (coalesce(mt.total_dose_limit_amt, NUMERIC '0') = coalesce(ms.total_dose_limit_amt, NUMERIC '0')
     AND coalesce(mt.total_dose_limit_amt, NUMERIC '1') = coalesce(ms.total_dose_limit_amt, NUMERIC '1'))
AND (coalesce(mt.daily_dose_limit_amt, NUMERIC '0') = coalesce(ms.daily_dose_limit_amt, NUMERIC '0')
     AND coalesce(mt.daily_dose_limit_amt, NUMERIC '1') = coalesce(ms.daily_dose_limit_amt, NUMERIC '1'))
AND (coalesce(mt.session_dose_limit_amt, NUMERIC '0') = coalesce(ms.session_dose_limit_amt, NUMERIC '0')
     AND coalesce(mt.session_dose_limit_amt, NUMERIC '1') = coalesce(ms.session_dose_limit_amt, NUMERIC '1'))
AND (upper(coalesce(mt.primary_ind, '0')) = upper(coalesce(ms.primary_ind, '0'))
     AND upper(coalesce(mt.primary_ind, '1')) = upper(coalesce(ms.primary_ind, '1')))
AND (coalesce(mt.log_id, 0) = coalesce(ms.log_id, 0)
     AND coalesce(mt.log_id, 1) = coalesce(ms.log_id, 1))
AND (coalesce(mt.run_id, 0) = coalesce(ms.run_id, 0)
     AND coalesce(mt.run_id, 1) = coalesce(ms.run_id, 1))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (fact_treatment_history_sk,
        patient_course_sk,
        patient_plan_sk,
        patient_sk,
        treatment_intent_type_id,
        clinical_status_id,
        plan_status_id,
        field_technique_id,
        technique_id,
        technique_label_id,
        technique_delivery_type_id,
        site_sk,
        source_fact_treatment_history_id,
        completion_date_time,
        first_treatment_date_time,
        last_treatment_date_time,
        status_date_time,
        active_ind,
        planned_dose_rate_num,
        course_dose_delivered_amt,
        course_dose_planned_amt,
        course_dose_remaining_amt,
        other_course_dose_delivered_amt,
        dose_correction_amt,
        total_dose_limit_amt,
        daily_dose_limit_amt,
        session_dose_limit_amt,
        primary_ind,
        log_id,
        run_id,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.fact_treatment_history_sk, ms.patient_course_sk, ms.patient_plan_sk, ms.patient_sk, ms.treatment_intent_type_id, ms.clinical_status_id, ms.plan_status_id, ms.field_technique_id, ms.technique_id, ms.technique_label_id, ms.technique_delivery_type_id, ms.site_sk, ms.source_fact_treatment_history_id, ms.completion_date_time, ms.first_treatment_date_time, ms.last_treatment_date_time, ms.status_date_time, ms.active_ind, ms.planned_dose_rate_num, ms.course_dose_delivered_amt, ms.course_dose_planned_amt, ms.course_dose_remaining_amt, ms.other_course_dose_delivered_amt, ms.dose_correction_amt, ms.total_dose_limit_amt, ms.daily_dose_limit_amt, ms.session_dose_limit_amt, ms.primary_ind, ms.log_id, ms.run_id, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT fact_treatment_history_sk
      FROM `hca-hin-dev-cur-ops`.edwcr.fact_rad_onc_treatment_history
      GROUP BY fact_treatment_history_sk
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.fact_rad_onc_treatment_history');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','Fact_rad_Onc_Treatment_History');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF;;