-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_patient_contact_detail.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT concat(format('%20d', count(*)), ',\t') AS source_string
FROM
  (SELECT stg.cn_patient_contact_sid,
          stg.contact_detail_measure_type_id,
          stg.contact_detail_measure_val_txt,
          stg.hashbite_ssk,
          stg.source_system_code,
          stg.dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_contact_detail_stg AS stg
   WHERE upper(stg.hashbite_ssk) NOT IN
       (SELECT upper(cn_patient_contact_detail.hashbite_ssk) AS hashbite_ssk
        FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_contact_detail) ) AS a