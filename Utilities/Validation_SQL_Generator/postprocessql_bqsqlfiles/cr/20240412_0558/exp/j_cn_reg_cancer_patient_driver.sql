-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_reg_cancer_patient_driver.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT row_number() OVER (
                             ORDER BY met.cr_patient_id,
                                      inav.nav_patient_id,
                                      cpid.patient_dw_id) AS cancer_patient_driver_sk,
                            met.cr_patient_id AS cr_patient_id,
                            inav.nav_patient_id AS cn_patient_id,
                            cpid.patient_dw_id AS cp_patient_id,
                            coalesce(met.coid, cpid.coid, inav.coid) AS coid,
                            coalesce(met.company_code, cpid.company_code, inav.company_code) AS company_code,
                            coalesce(met.network_mnemonic_cs, cpid.network_mnemonic_cs, inav.network_mnemonic_cs) AS network_mnemonic_cs,
                            coalesce(met.patient_market_urn_text, cpid.patient_market_urn, inav.empi_text) AS patient_market_urn_text,
                            coalesce(met.medical_record_num, cpid.medical_record_num, inav.medical_record_num) AS medical_record_num,
                            empi.empi_text,
                            coalesce(met.patient_first_name, cpid.patient_first_name, inav.patient_first_name) AS patient_first_name,
                            coalesce(met.patient_middle_name, inav.patient_middle_name) AS patient_middle_name,
                            coalesce(met.patient_last_name, cpid.patient_last_name, inav.patient_last_name) AS patient_last_name,
                            inav.preferred_name,
                            coalesce(met.source_system_code, cpid.source_system_code, inav.source_system_code) AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT cp.patient_market_urn_text,
             cp.cr_patient_id,
             cp.coid,
             cp.company_code,
             cp.medical_record_num,
             df.facility_mnemonic_cs,
             df.network_mnemonic_cs,
             cp.patient_first_name AS patient_first_name,
             cp.patient_last_name AS patient_last_name,
             cp.patient_middle_name AS patient_middle_name,
             cp.source_system_code
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient AS cp
      INNER JOIN
        (SELECT clinical_facility.facility_mnemonic_cs,
                clinical_facility.network_mnemonic_cs,
                clinical_facility.coid,
                clinical_facility.facility_active_ind
         FROM `hca-hin-dev-cur-ops`.edw_pub_views.clinical_facility QUALIFY row_number() OVER (PARTITION BY upper(clinical_facility.coid)
                                                                                               ORDER BY upper(clinical_facility.facility_active_ind) DESC) = 1) AS df ON upper(rtrim(cp.coid)) = upper(rtrim(df.coid))) AS met
   FULL OUTER JOIN
     (SELECT crio.patient_market_urn,
             crio.network_mnemonic_cs,
             crio.medical_record_num,
             crio.patient_dw_id,
             crio.coid,
             crio.company_code,
             crio.first_name AS patient_first_name,
             crio.last_name AS patient_last_name,
             df1.facility_mnemonic_cs,
             crio.source_system_code
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output AS crio
      INNER JOIN
        (SELECT clinical_facility.facility_mnemonic_cs,
                clinical_facility.network_mnemonic_cs,
                clinical_facility.coid,
                clinical_facility.facility_active_ind
         FROM `hca-hin-dev-cur-ops`.edw_pub_views.clinical_facility QUALIFY row_number() OVER (PARTITION BY upper(clinical_facility.coid)
                                                                                               ORDER BY upper(clinical_facility.facility_active_ind) DESC) = 1) AS df1 ON upper(rtrim(crio.coid)) = upper(rtrim(df1.coid)) QUALIFY row_number() OVER (PARTITION BY coalesce(crio.patient_market_urn, concat(crio.medical_record_num, df1.facility_mnemonic_cs), format('%#20.0f', crio.patient_dw_id))
                                                                                                                                                                                                                                                      ORDER BY crio.report_assigned_date_time DESC) = 1) AS cpid ON rtrim(coalesce(met.patient_market_urn_text, concat(met.medical_record_num, met.facility_mnemonic_cs))) = rtrim(coalesce(cpid.patient_market_urn, concat(cpid.medical_record_num, cpid.facility_mnemonic_cs)))
   AND rtrim(met.network_mnemonic_cs) = rtrim(cpid.network_mnemonic_cs)
   FULL OUTER JOIN
     (SELECT cp.nav_patient_id,
             cpt.patient_market_urn,
             cpt.network_mnemonic_cs,
             cpt.medical_record_num,
             cpt.company_code,
             cpt.empi_text,
             cpt.coid,
             df2.facility_mnemonic_cs,
             cp.first_name AS patient_first_name,
             cp.last_name AS patient_last_name,
             cp.middle_name AS patient_middle_name,
             cp.preferred_name,
             cp.source_system_code
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cn_person AS cp
      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient AS cpt ON cp.nav_patient_id = cpt.nav_patient_id
      INNER JOIN
        (SELECT clinical_facility.facility_mnemonic_cs,
                clinical_facility.network_mnemonic_cs,
                clinical_facility.coid,
                clinical_facility.facility_active_ind
         FROM `hca-hin-dev-cur-ops`.edw_pub_views.clinical_facility QUALIFY row_number() OVER (PARTITION BY upper(clinical_facility.coid)
                                                                                               ORDER BY upper(clinical_facility.facility_active_ind) DESC) = 1) AS df2 ON upper(rtrim(cpt.coid)) = upper(rtrim(df2.coid))
      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_address AS cpa ON cp.nav_patient_id = cpa.nav_patient_id
      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_phone_num AS cpp ON cp.nav_patient_id = cpp.nav_patient_id QUALIFY row_number() OVER (PARTITION BY coalesce(cpt.patient_market_urn, concat(cpt.medical_record_num, df2.facility_mnemonic_cs), format('%#20.0f', cp.nav_patient_id))
                                                                                                                                                              ORDER BY cp.nav_patient_id DESC) = 1) AS inav ON rtrim(coalesce(concat(trim(met.network_mnemonic_cs), met.patient_market_urn_text), concat(met.medical_record_num, met.facility_mnemonic_cs))) = rtrim(coalesce(inav.patient_market_urn, concat(inav.medical_record_num, inav.facility_mnemonic_cs)))
   AND rtrim(met.network_mnemonic_cs) = rtrim(inav.network_mnemonic_cs)
   LEFT OUTER JOIN
     (SELECT a.patient_market_urn,
             a.network_mnemonic_cs,
             a.empi_text
      FROM
        (SELECT cr.patient_market_urn,
                cr.network_mnemonic_cs,
                e.empi_text
         FROM `hca-hin-dev-cur-ops`.edwcr_views.clinical_registration AS cr
         INNER JOIN `hca-hin-dev-cur-ops`.edwcr_views.empi_encounter_id_xwalk AS e ON cr.patient_dw_id = e.encounter_id
         WHERE upper(rtrim(e.encounter_id_type_name)) = 'PATIENT_DW_ID'
           AND CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(e.empi_text) AS FLOAT64) > 0 ) AS a QUALIFY row_number() OVER (PARTITION BY upper(a.patient_market_urn),
                                                                                                                                                        upper(a.network_mnemonic_cs)
                                                                                                                                           ORDER BY upper(a.empi_text)) = 1) AS empi ON rtrim(coalesce(met.network_mnemonic_cs, cpid.network_mnemonic_cs, inav.network_mnemonic_cs)) = rtrim(empi.network_mnemonic_cs)
   AND upper(rtrim(coalesce(met.patient_market_urn_text, cpid.patient_market_urn, inav.empi_text))) = upper(rtrim(empi.patient_market_urn))) AS b