-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_patient_surgery_recon.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT concat(format('%20d', count(*)), ',\t') AS source_string
FROM
  (SELECT stg.cn_patient_surgery_recstr_sid,
          stg.core_record_type_id,
          rs.side_id,
          stg.med_spcl_physician_id,
          stg.nav_patient_id,
          stg.tumor_type_id,
          stg.diagnosis_result_id,
          stg.nav_diagnosis_id,
          stg.navigator_id,
          stg.coid,
          stg.company_code,
          stg.recstr_date,
          stg.surgery_recstr_type_text,
          stg.declined_ind,
          stg.hashbite_ssk,
          stg.source_system_code,
          stg.dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_surgery_reconstruction_stg AS stg
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_side AS rs ON upper(trim(stg.reconsurgeryside)) = upper(trim(rs.side_desc))
   WHERE upper(stg.hashbite_ssk) NOT IN
       (SELECT upper(cn_patient_surgery_reconstruction.hashbite_ssk) AS hashbite_ssk
        FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_surgery_reconstruction) ) AS a