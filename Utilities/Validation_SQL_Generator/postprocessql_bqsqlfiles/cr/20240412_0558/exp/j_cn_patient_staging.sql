-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_patient_staging.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT concat(format('%20d', count(*)), ', ') AS source_string
FROM
  (SELECT cn_patient_staging_stg.cn_patient_staging_sid,
          cn_patient_staging_stg.hashbite_ssk
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_staging_stg
   WHERE (upper(cn_patient_staging_stg.hashbite_ssk),
          upper(cn_patient_staging_stg.cancer_stage_class_method_code)) IN
       (SELECT AS STRUCT upper(cn_patient_staging.hashbite_ssk) AS hashbite_ssk,
                         upper(cn_patient_staging.cancer_stage_class_method_code) AS cancer_stage_class_method_code
        FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_staging)
     AND cn_patient_staging_stg.dw_last_update_date_time <
       (SELECT max(etl_job_run.job_start_date_time) AS job_start_date_time
        FROM `hca-hin-dev-cur-ops`.edwcr_dmx_ac_base_views.etl_job_run
        WHERE upper(rtrim(etl_job_run.job_name)) = 'J_CN_PATIENT_STAGING' ) ) AS a