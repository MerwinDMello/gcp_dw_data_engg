-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_physician_role.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT cn_physician_role_stg.physician_id,
          cn_physician_role_stg.physician_role_code
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_physician_role_stg
   UNION DISTINCT SELECT cn_physician_detail.physician_id,
                         'Gyn' AS physician_role_code
   FROM `hca-hin-dev-cur-ops`.edwcr.cn_physician_detail
   INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cn_patient ON cn_physician_detail.physician_id = cn_patient.gynecologist_physician_id
   UNION DISTINCT SELECT cn_physician_detail.physician_id,
                         'PCP' AS physician_role_code
   FROM `hca-hin-dev-cur-ops`.edwcr.cn_physician_detail
   INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cn_patient ON cn_physician_detail.physician_id = cn_patient.primary_care_physician_id
   UNION DISTINCT SELECT cn_physician_detail.physician_id,
                         'ETP' AS physician_role_code
   FROM `hca-hin-dev-cur-ops`.edwcr.cn_physician_detail
   INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cn_patient_tumor ON cn_physician_detail.physician_id = cn_patient_tumor.treatment_end_physician_id) AS ab
WHERE (ab.physician_id,
       upper(ab.physician_role_code)) NOT IN
    (SELECT AS STRUCT cn_physician_role.physician_id,
                      upper(cn_physician_role.physician_role_code) AS physician_role_code
     FROM `hca-hin-dev-cur-ops`.edwcr.cn_physician_role)