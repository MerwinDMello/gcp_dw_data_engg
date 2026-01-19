-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/act/j_cr_ro_fact_rad_onc_patient.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT row_number() OVER (
                             ORDER BY dhd.dimsiteid,
                                      dhd.factpatientid) AS fact_patient_sk,
                            rpp.hospital_sk AS hospital_sk,
                            rpa.patient_sk AS patient_sk,
                            dhd.patient_status_id AS patient_status_id,
                            dhd.location_sk,
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
                      stg_factpatient.dimlookupid_patientstatus AS patient_status_id,
                      stg_factpatient.dimlocationid AS location_sk,
                      stg_factpatient.dimlookupid_race AS race_id,
                      stg_factpatient.dimlookupid_gender AS gender_id,
                      CAST(trim(stg_factpatient.patientcreationdate) AS DATETIME) AS creation_date_time,
                      CAST(trim(stg_factpatient.patientadmissiondate) AS DATETIME) AS admission_date_time,
                      CAST(trim(stg_factpatient.patientdischargedate) AS DATETIME) AS discharge_date_time,
                      stg_factpatient.logid AS log_id,
                      stg_factpatient.runid AS run_id
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_factpatient) AS dhd
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_rad_onc_hospital AS rpp ON dhd.dimhospitaldepartmentid = rpp.source_hospital_id
   AND dhd.dimsiteid = rpp.site_sk
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.rad_onc_patient AS rpa ON dhd.dimpatientid = rpa.source_patient_id
   AND dhd.dimsiteid = rpa.site_sk
   INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_rad_onc_site AS rr ON rr.source_site_id = dhd.dimsiteid) AS ds
WHERE ds.dw_last_update_date_time >=
    (SELECT max(etl_job_run.job_start_date_time) AS job_start_date_time
     FROM `hca-hin-dev-cur-ops`.edwcr_dmx_ac.etl_job_run
     WHERE upper(rtrim(etl_job_run.job_name)) = 'J_CR_RO_FACT_RAD_ONC_PATIENT' )