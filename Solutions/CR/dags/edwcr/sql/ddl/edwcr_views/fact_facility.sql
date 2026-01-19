-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/fact_facility.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.fact_facility AS SELECT
    a.company_code,
    coid,
    corporate_level_num,
    corporate_type_code,
    corporate_type_name,
    corporate_code,
    corporate_name,
    sector_level_num,
    sector_type_code,
    sector_type_name,
    sector_code,
    sector_name,
    group_level_num,
    group_type_code,
    group_type_name,
    group_code,
    group_name,
    division_level_num,
    division_type_code,
    division_type_name,
    division_code,
    division_name,
    market_level_num,
    market_type_code,
    market_type_name,
    market_code,
    market_name,
    cons_facility_level_num,
    cons_facility_type_code,
    cons_facility_type_name,
    cons_facility_code,
    cons_facility_name,
    coid_level_num,
    coid_type_code,
    coid_type_name,
    coid_name,
    coid_status_code,
    unit_num,
    lob_code,
    lob_name,
    sub_lob_code,
    sub_lob_name,
    financial_jv_code,
    own_managed_code,
    own_percentage,
    income_category,
    income_category_code,
    cons_income_category_code,
    gl_consolidation_ind,
    gl_standard_chart_id,
    state_code,
    fiscal_year_end_month,
    medicare_year_end_month,
    data_source_code,
    pas_coid,
    pas_status,
    company_code_operations
  FROM
    {{ params.param_cr_base_views_dataset_name }}.fact_facility AS a
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
