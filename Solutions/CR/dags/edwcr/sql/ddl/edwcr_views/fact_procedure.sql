-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/fact_procedure.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.fact_procedure AS SELECT
    procedure_sk,
    patient_dw_id,
    a.company_code,
    a.coid,
    valid_from_date_time,
    valid_to_date_time,
    encounter_sk,
    priority_sequence,
    procedure_code,
    procedure_desc,
    procedure_date_time,
    anesthesia_sk,
    source_system_code,
    dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.fact_procedure AS a
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
