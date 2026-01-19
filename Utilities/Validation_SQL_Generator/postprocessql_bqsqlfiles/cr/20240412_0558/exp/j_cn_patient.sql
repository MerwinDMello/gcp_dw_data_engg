-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_patient.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT stg.nav_patient_id,
          stg.navigator_id,
          CASE
              WHEN trim(stg.coid) IS NULL THEN '-1'
              ELSE stg.coid
          END AS coid,
          'H' AS company_code,
          stg.patient_market_urn,
          stg.medical_record_num,
          stg.empi_text,
          pd1.physician_id AS gynecologist_physician_id,
          pd2.physician_id AS primary_care_physician_id,
          cf.facility_mnemonic_cs AS facility_mnemonic_cs,
          cf.network_mnemonic_cs AS network_mnemonic_cs,
          stg.nav_create_date,
          'N' AS source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_stg AS stg
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.cn_physician_detail AS pd1 ON upper(rtrim(coalesce(trim(stg.gynecologist), 'X'))) = upper(rtrim(coalesce(trim(pd1.physician_name), 'X')))
   AND upper(rtrim(coalesce(trim(stg.gynecologistphone), 'X'))) = upper(rtrim(coalesce(trim(pd1.physician_phone_num), 'X')))
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.cn_physician_detail AS pd2 ON upper(rtrim(coalesce(trim(stg.primarycarephysician), 'XX'))) = upper(rtrim(coalesce(trim(pd2.physician_name), 'XX')))
   AND upper(rtrim(coalesce(trim(stg.pcpphone), 'XX'))) = upper(rtrim(coalesce(trim(pd2.physician_phone_num), 'XX')))
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edw_pub_views.clinical_facility AS cf ON upper(rtrim(stg.coid)) = upper(rtrim(cf.coid))
   AND upper(rtrim(cf.facility_active_ind)) = 'Y'
   WHERE stg.nav_patient_id NOT IN
       (SELECT -- where STG.COID is NOT NULL
 cn_patient.nav_patient_id
        FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient) QUALIFY row_number() OVER (PARTITION BY stg.nav_patient_id
                                                                                ORDER BY primary_care_physician_id DESC) = 1 ) AS src