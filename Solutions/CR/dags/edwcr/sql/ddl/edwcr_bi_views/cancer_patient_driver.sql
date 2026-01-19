-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/edwcr_bi_views/cancer_patient_driver.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_bi_views_dataset_name }}.cancer_patient_driver AS SELECT
    a.cancer_patient_driver_sk,
    a.cr_patient_id,
    a.cp_patient_id,
    a.cn_patient_id,
    a.coid,
    a.company_code,
    a.network_mnemonic_cs,
    a.patient_market_urn_text,
    a.medical_record_num,
    a.empi_text,
    CASE
      WHEN session_user() = pn.userid THEN '***'
      ELSE a.patient_first_name
    END AS patient_first_name,
    CASE
      WHEN session_user() = pn.userid THEN '***'
      ELSE a.patient_middle_name
    END AS patient_middle_name,
    CASE
      WHEN session_user() = pn.userid THEN '***'
      ELSE a.patient_last_name
    END AS patient_last_name,
    a.preferred_name,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS a
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
    LEFT OUTER JOIN (
      SELECT
          security_mask_and_exception.userid,
          security_mask_and_exception.masked_column_code
        FROM
          {{ params.param_auth_base_views_dataset_name }}.security_mask_and_exception
        WHERE security_mask_and_exception.userid = session_user()
         AND upper(security_mask_and_exception.masked_column_code) = 'PN'
    ) AS pn ON pn.userid = session_user()
;
