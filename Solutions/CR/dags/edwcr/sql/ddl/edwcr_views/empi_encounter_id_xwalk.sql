-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/empi_encounter_id_xwalk.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.empi_encounter_id_xwalk AS SELECT
    encounter_id,
    source_system_code,
    empi_text,
    pat_acct_num,
    encounter_subject_area_name,
    encounter_id_type_name,
    fa.coid,
    fa.company_code,
    dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.empi_encounter_id_xwalk AS fa
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON b.co_id = fa.coid
     AND b.company_code = fa.company_code
     AND b.user_id = session_user()
;
