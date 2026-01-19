-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_navque_patient_tumor_driver.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM `hca-hin-dev-cur-ops`.edwcr_base_views.navque_history AS a
LEFT OUTER JOIN
  (SELECT cpio.*
   FROM
     (SELECT DISTINCT a_0.cancer_patient_driver_sk,
                      a_0.message_control_id_text,
                      a_0.user_action_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output_driver AS a_0) AS cpio QUALIFY row_number() OVER (PARTITION BY upper(cpio.message_control_id_text)
                                                                                                                             ORDER BY cpio.user_action_date_time DESC) = 1) AS sk ON upper(rtrim(a.message_control_id_text)) = upper(rtrim(sk.message_control_id_text))
LEFT OUTER JOIN
  (SELECT cancer_tumor_driver.cp_icd_oncology_code,
          cancer_tumor_driver.cn_navque_tumor_type_id,
          cancer_tumor_driver.cancer_tumor_driver_sk
   FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_tumor_driver QUALIFY row_number() OVER (PARTITION BY cancer_tumor_driver.cn_navque_tumor_type_id
                                                                                              ORDER BY cancer_tumor_driver.cancer_tumor_driver_sk DESC) = 1) AS ctd ON a.tumor_type_id = ctd.cn_navque_tumor_type_id