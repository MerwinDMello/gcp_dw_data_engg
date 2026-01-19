-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cr_ro_fact_rad_onc_treatment_history.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT row_number() OVER (
                             ORDER BY CAST(dp.dimsiteid AS INT64),
                                      CAST(dp.facttreatmenthistoryid AS INT64)) AS fact_treatment_history_sk,
                            ra.patient_course_sk AS patient_course_sk,
                            pp.patient_plan_sk AS patient_plan_sk,
                            ra1.patient_sk AS patient_sk,
                            dp.dimlkpid_treatmentintenttype,
                            dp.dimlookupid_clinicalstatus,
                            dp.dimlookupid_planstatus,
                            dp.dimlookupid_fieldtechnique,
                            dp.dimlookupid_technique,
                            dp.dimlookupid_techniquelabel,
                            dp.dimlkpid_treatmentdeliverytyp,
                            rr.site_sk AS site_sk,
                            dp.facttreatmenthistoryid,
                            dp.completion_date_time,
                            dp.first_treatment_date_time,
                            dp.last_treatment_date_time,
                            dp.status_date_time,
                            dp.active_ind,
                            dp.planneddoserate,
                            dp.coursedosedelivered,
                            dp.coursedoseplanned,
                            dp.coursedoseremaining,
                            dp.othercoursedosedelivered,
                            dp.dosecorrection,
                            dp.totaldoselimit,
                            dp.dailydoselimit,
                            dp.sessiondoselimit,
                            dp.primary_ind,
                            dp.logid,
                            dp.runid,
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
      FROM {{ params.param_cr_stage_dataset_name }}.facttreatmenthist_stg) AS dp
   INNER JOIN
     (SELECT ref_rad_onc_site.source_site_id,
             ref_rad_onc_site.site_sk
      FROM {{ params.param_cr_base_views_dataset_name }}.ref_rad_onc_site) AS rr ON rr.source_site_id = dp.dimsiteid
   LEFT OUTER JOIN
     (SELECT rad_onc_patient_course.patient_course_sk AS patient_course_sk,
             rad_onc_patient_course.source_patient_course_id,
             rad_onc_patient_course.site_sk
      FROM {{ params.param_cr_base_views_dataset_name }}.rad_onc_patient_course) AS ra ON dp.dimcourseid = ra.source_patient_course_id
   AND rr.site_sk = ra.site_sk
   LEFT OUTER JOIN
     (SELECT rad_onc_patient_plan.source_patient_plan_id,
             rad_onc_patient_plan.site_sk,
             rad_onc_patient_plan.patient_plan_sk
      FROM {{ params.param_cr_base_views_dataset_name }}.rad_onc_patient_plan) AS pp ON dp.dimplanid = pp.source_patient_plan_id
   AND rr.site_sk = pp.site_sk
   LEFT OUTER JOIN
     (SELECT rad_onc_patient.source_patient_id,
             rad_onc_patient.site_sk,
             rad_onc_patient.patient_sk
      FROM {{ params.param_cr_base_views_dataset_name }}.rad_onc_patient) AS ra1 ON dp.dimpatientid = ra1.source_patient_id
   AND rr.site_sk = ra1.site_sk) AS stg