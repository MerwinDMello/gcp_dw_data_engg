-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_patient_tumor.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT stg.cn_patient_tumor_sid,
          stg.nav_patient_id,
          stg.tumor_type_id,
          stg.navigator_id,
          stg.coid,
          'H' AS company_code,
          stg.electronic_folder_id_text,
          rf1.facility_id AS referral_source_facility_id,
          rs.status_id AS nav_status_id,
          stg.identification_period_text,
          stg.referral_date,
          stg.referring_physician_id,
          stg.nav_end_reason_text,
          stg.treatment_end_reason_text,
          pd.physician_id AS treatment_end_physician_id,
          rf2.facility_id AS treatment_end_facility_id,
          stg.hashbite_ssk,
          'N' AS source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_cr_stage_dataset_name }}.cn_patient_tumor_stg AS stg
   LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_facility AS rf1
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(rtrim(stg.tumorreferralsource)) = upper(rtrim(rf1.facility_name))
   LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_status AS rs
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(rtrim(stg.navigationstatus)) = upper(rtrim(rs.status_desc))
   LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.cn_physician_detail AS pd
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(rtrim(stg.endtreatmentphysician)) = upper(rtrim(pd.physician_name))
   AND pd.physician_phone_num IS NULL
   LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_facility AS rf2
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(rtrim(stg.endtreatmentlocation)) = upper(rtrim(rf2.facility_name))
   WHERE upper(stg.hashbite_ssk) NOT IN
       (SELECT upper(cn_patient_tumor.hashbite_ssk) AS hashbite_ssk
        FROM {{ params.param_cr_core_dataset_name }}.cn_patient_tumor
        FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')) QUALIFY row_number() OVER (PARTITION BY stg.cn_patient_tumor_sid
                                                                                                        ORDER BY treatment_end_physician_id DESC) = 1 ) AS src