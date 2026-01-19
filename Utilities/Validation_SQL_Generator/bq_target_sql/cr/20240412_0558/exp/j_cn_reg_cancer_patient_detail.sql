-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_reg_cancer_patient_detail.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', count(*)) AS source_string
  FROM
    (
      SELECT
          row_number() OVER (ORDER BY upper(met.patient_market_urn_text), met.network_mnemonic_cs) AS cancer_patient_detail_sk,
          coalesce(cp.cancer_patient_driver_sk, cpp.cancer_patient_driver_sk, cpn.cancer_patient_driver_sk) AS cancer_patient_driver_sk,
          met.cr_patient_id AS cr_patient_id,
          inav.nav_patient_id AS cn_patient_id,
          cpid.patient_dw_id AS cp_patient_id,
          coalesce(met.coid, cpid.coid, inav.coid) AS coid,
          coalesce(met.company_code, cpid.company_code, inav.company_code) AS company_code,
          coalesce(met.network_mnemonic_cs, cpid.network_mnemonic_cs, inav.network_mnemonic_cs) AS network_mnemonic_cs,
          coalesce(met.patient_market_urn_text, cpid.patient_market_urn, inav.empi_text) AS patient_market_urn_text,
          coalesce(met.medical_record_num, cpid.medical_record_num, inav.medical_record_num) AS medical_record_num,
          met.relation_name AS relationship_name,
          coalesce(met.address_line_1_text, cpid.address_line_1_text, inav.address_line_1_text) AS address_line_1_text,
          coalesce(met.address_line_2_text, cpid.address_line_2_text, inav.address_line_2_text) AS address_line_2_text,
          coalesce(met.city_name, cpid.city_name, inav.city_name) AS city_name,
          inav.death_date,
          coalesce(met.patient_birth_date, cpid.patient_birth_date, inav.patient_birth_date) AS patient_birth_date,
          coalesce(met.patient_email_address_text, inav.patient_email_address_text) AS patient_email_address_text,
          coalesce(met.gender_code, cpid.gender_code, inav.gender_code) AS gender_code,
          coalesce(met.state_code, cpid.state_code, inav.state_code) AS state_code,
          coalesce(met.zip_code, cpid.zip_code, inav.zip_code) AS zip_code,
          coalesce(met.phone_num_type_code, inav.phone_num_type_code) AS phone_num_type_code,
          coalesce(met.phone_num, inav.phone_num) AS phone_num,
          inav.preferred_language_text,
          -- -INAV.PREFERRED_NAME,
          met.insurance_type_name,
          met.preferred_contact_method_text,
          met.insurance_company_name,
          met.race_name,
          met.patient_system_id,
          met.vital_status_name,
          coalesce(met.source_system_code, cpid.source_system_code, inav.source_system_code) AS source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
        FROM
          (
            SELECT DISTINCT
                cp_0.patient_market_urn_text,
                cp_0.cr_patient_id,
                cp_0.coid,
                cp_0.company_code,
                cp_0.medical_record_num,
                rel.lookup_desc AS relation_name,
                df.network_mnemonic_cs,
                df.facility_mnemonic_cs,
                cppn.phone_num_type_code AS phone_num_type_code,
                cppn.phone_num AS phone_num,
                cpa.address_line_1_text AS address_line_1_text,
                cpa.address_line_2_text AS address_line_2_text,
                cpa.city_name AS city_name,
                cp_0.patient_birth_date AS patient_birth_date,
                cp_0.patient_email_address_text AS patient_email_address_text,
                cp_0.patient_first_name AS patient_first_name,
                CASE
                   upper(rtrim(gn.lookup_desc))
                  WHEN 'MALE' THEN 'M'
                  WHEN 'FEMALE' THEN 'F'
                  ELSE 'U'
                END AS gender_code,
                cp_0.patient_last_name AS patient_last_name,
                cp_0.patient_middle_name AS patient_middle_name,
                st.lookup_code AS state_code,
                cpa.zip_code AS zip_code,
                ici.lookup_desc AS insurance_type_name,
                cpc.preferred_contact_method_text AS preferred_contact_method_text,
                iti.lookup_desc AS insurance_company_name,
                rc.lookup_desc AS race_name,
                cp_0.patient_system_id AS patient_system_id,
                vs.lookup_desc AS vital_status_name,
                cp_0.source_system_code
              FROM
                `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient AS cp_0
                INNER JOIN (
                  SELECT
                      clinical_facility.facility_mnemonic_cs,
                      clinical_facility.network_mnemonic_cs,
                      clinical_facility.coid,
                      clinical_facility.facility_active_ind
                    FROM
                      `hca-hin-dev-cur-ops`.edw_pub_views.clinical_facility
                    QUALIFY row_number() OVER (PARTITION BY upper(clinical_facility.coid) ORDER BY upper(clinical_facility.facility_active_ind) DESC) = 1
                ) AS df ON upper(rtrim(cp_0.coid)) = upper(rtrim(df.coid))
                LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_address AS cpa ON cp_0.cr_patient_id = cpa.cr_patient_id
                LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_contact AS cpc ON cp_0.cr_patient_id = cpc.cr_patient_id
                LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_phone_num AS cppn ON cp_0.cr_patient_id = cppn.cr_patient_id
                 AND cpc.patient_contact_id = cppn.patient_contact_id
                LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_insurance AS cpi ON cp_0.cr_patient_id = cpi.cr_patient_id
                LEFT OUTER JOIN (
                  SELECT
                      ref_lookup_code.master_lookup_sid,
                      ref_lookup_code.lookup_desc
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code
                    WHERE ref_lookup_code.lookup_id = 1
                ) AS vs ON cp_0.vital_status_id = vs.master_lookup_sid
                LEFT OUTER JOIN (
                  SELECT
                      ref_lookup_code.master_lookup_sid,
                      ref_lookup_code.lookup_desc
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code
                    WHERE ref_lookup_code.lookup_id = 2
                ) AS rc ON cp_0.patient_race_id = rc.master_lookup_sid
                LEFT OUTER JOIN (
                  SELECT
                      ref_lookup_code.master_lookup_sid,
                      ref_lookup_code.lookup_desc
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code
                    WHERE ref_lookup_code.lookup_id = 3
                ) AS gn ON cp_0.patient_gender_id = gn.master_lookup_sid
                LEFT OUTER JOIN (
                  SELECT
                      ref_lookup_code.master_lookup_sid,
                      ref_lookup_code.lookup_desc
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code
                    WHERE ref_lookup_code.lookup_id = 4
                ) AS rel ON cpc.contact_relation_id = rel.master_lookup_sid
                LEFT OUTER JOIN (
                  SELECT
                      ref_lookup_code.master_lookup_sid,
                      ref_lookup_code.lookup_desc
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code
                    WHERE ref_lookup_code.lookup_id = 13
                ) AS iti ON cpi.insurance_type_id = iti.master_lookup_sid
                LEFT OUTER JOIN (
                  SELECT
                      ref_lookup_code.master_lookup_sid,
                      ref_lookup_code.lookup_desc
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code
                    WHERE ref_lookup_code.lookup_id = 14
                ) AS ici ON cpi.insurance_company_id = ici.master_lookup_sid
                LEFT OUTER JOIN (
                  SELECT
                      ref_lookup_code.master_lookup_sid,
                      ref_lookup_code.lookup_code,
                      ref_lookup_code.lookup_desc
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code
                    WHERE ref_lookup_code.lookup_id = 23
                ) AS st ON cpa.state_id = st.master_lookup_sid
          ) AS met
          FULL OUTER JOIN (
            SELECT
                crio.patient_market_urn,
                crio.network_mnemonic_cs,
                crio.patient_dw_id,
                crio.coid,
                crio.medical_record_num,
                crio.address_line_1_text,
                df1.facility_mnemonic_cs,
                crio.company_code,
                crio.address_line_2_text,
                crio.city_name,
                crio.birth_date AS patient_birth_date,
                crio.first_name AS patient_first_name,
                crio.gender_code,
                crio.last_name AS patient_last_name,
                crio.state_code,
                crio.zip_code,
                crio.source_system_code
              FROM
                `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output AS crio
                INNER JOIN (
                  SELECT
                      clinical_facility.facility_mnemonic_cs,
                      clinical_facility.network_mnemonic_cs,
                      clinical_facility.coid,
                      clinical_facility.facility_active_ind
                    FROM
                      `hca-hin-dev-cur-ops`.edw_pub_views.clinical_facility
                    QUALIFY row_number() OVER (PARTITION BY upper(clinical_facility.coid) ORDER BY upper(clinical_facility.facility_active_ind) DESC) = 1
                ) AS df1 ON upper(rtrim(crio.coid)) = upper(rtrim(df1.coid))
              QUALIFY row_number() OVER (PARTITION BY coalesce(crio.patient_market_urn, concat(crio.medical_record_num, df1.facility_mnemonic_cs), format('%#20.0f', crio.patient_dw_id)) ORDER BY crio.report_assigned_date_time DESC) = 1
          ) AS cpid ON rtrim(coalesce(met.patient_market_urn_text, concat(met.medical_record_num, met.facility_mnemonic_cs))) = rtrim(coalesce(cpid.patient_market_urn, concat(cpid.medical_record_num, cpid.facility_mnemonic_cs)))
           AND rtrim(met.network_mnemonic_cs) = rtrim(cpid.network_mnemonic_cs)
          FULL OUTER JOIN (
            SELECT
                cp_0.nav_patient_id,
                cpt.patient_market_urn,
                df2.network_mnemonic_cs,
                cpt.coid,
                df2.facility_mnemonic_cs,
                cpt.company_code,
                cpt.empi_text,
                cpt.medical_record_num,
                cpa.address_line_1_text,
                cpa.address_line_2_text,
                cpa.city_name,
                cp_0.death_date,
                cp_0.birth_date AS patient_birth_date,
                cp_0.patient_email_text AS patient_email_address_text,
                cp_0.first_name AS patient_first_name,
                cp_0.gender_code,
                cp_0.last_name AS patient_last_name,
                cp_0.middle_name AS patient_middle_name,
                cpp_0.phone_num,
                cpp_0.phone_num_type_code,
                cp_0.preferred_language_text,
                cp_0.preferred_name,
                cpa.state_code,
                cpa.zip_code,
                cp_0.source_system_code
              FROM
                `hca-hin-dev-cur-ops`.edwcr_base_views.cn_person AS cp_0
                LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient AS cpt ON cp_0.nav_patient_id = cpt.nav_patient_id
                INNER JOIN (
                  SELECT
                      clinical_facility.facility_mnemonic_cs,
                      clinical_facility.network_mnemonic_cs,
                      clinical_facility.coid,
                      clinical_facility.facility_active_ind
                    FROM
                      `hca-hin-dev-cur-ops`.edw_pub_views.clinical_facility
                    QUALIFY row_number() OVER (PARTITION BY upper(clinical_facility.coid) ORDER BY upper(clinical_facility.facility_active_ind) DESC) = 1
                ) AS df2 ON upper(rtrim(cpt.coid)) = upper(rtrim(df2.coid))
                LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_address AS cpa ON cp_0.nav_patient_id = cpa.nav_patient_id
                LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_phone_num AS cpp_0 ON cp_0.nav_patient_id = cpp_0.nav_patient_id
              QUALIFY row_number() OVER (PARTITION BY coalesce(cpt.patient_market_urn, concat(cpt.medical_record_num, df2.facility_mnemonic_cs), format('%#20.0f', cp_0.nav_patient_id)) ORDER BY cp_0.nav_patient_id DESC) = 1
          ) AS inav ON rtrim(coalesce(concat(trim(met.network_mnemonic_cs), met.patient_market_urn_text), concat(met.medical_record_num, met.facility_mnemonic_cs))) = rtrim(coalesce(inav.patient_market_urn, concat(inav.medical_record_num, inav.facility_mnemonic_cs)))
           AND rtrim(met.network_mnemonic_cs) = rtrim(inav.network_mnemonic_cs)
          LEFT OUTER JOIN (
            SELECT
                cancer_patient_driver.cr_patient_id,
                cancer_patient_driver.cancer_patient_driver_sk
              FROM
                `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_driver
              QUALIFY row_number() OVER (PARTITION BY cancer_patient_driver.cr_patient_id ORDER BY cancer_patient_driver.cancer_patient_driver_sk) = 1
          ) AS cp ON met.cr_patient_id = cp.cr_patient_id
          LEFT OUTER JOIN (
            SELECT
                cancer_patient_driver.cn_patient_id,
                cancer_patient_driver.cancer_patient_driver_sk
              FROM
                `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_driver
              QUALIFY row_number() OVER (PARTITION BY cancer_patient_driver.cn_patient_id ORDER BY cancer_patient_driver.cancer_patient_driver_sk) = 1
          ) AS cpn ON inav.nav_patient_id = cpn.cn_patient_id
          LEFT OUTER JOIN (
            SELECT
                cancer_patient_driver.cp_patient_id,
                cancer_patient_driver.cancer_patient_driver_sk
              FROM
                `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_driver
              QUALIFY row_number() OVER (PARTITION BY cancer_patient_driver.cp_patient_id ORDER BY cancer_patient_driver.cancer_patient_driver_sk) = 1
          ) AS cpp ON cpid.patient_dw_id = cpp.cp_patient_id
    ) AS a
;
