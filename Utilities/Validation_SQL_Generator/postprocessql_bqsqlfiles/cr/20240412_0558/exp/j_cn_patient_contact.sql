-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_patient_contact.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT concat(format('%20d', count(*)), ',\t') AS source_string
FROM
  (SELECT cn_patient_contact_stg.cn_patient_contact_sid,
          cn_patient_contact_stg.hashbite_ssk
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_contact_stg
   WHERE upper(cn_patient_contact_stg.hashbite_ssk) NOT IN
       (SELECT upper(cn_patient_contact.hashbite_ssk) AS hashbite_ssk
        FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_contact)
     AND cn_patient_contact_stg.dw_last_update_date_time <
       (SELECT max(etl_job_run.job_start_date_time) AS job_start_date_time
        FROM `hca-hin-dev-cur-ops`.edwcr_dmx_ac.etl_job_run
        WHERE upper(rtrim(etl_job_run.job_name)) = 'J_CN_PATIENT_CONTACT' ) ) AS a