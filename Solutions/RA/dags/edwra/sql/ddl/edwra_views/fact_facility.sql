-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/fact_facility.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.fact_facility AS SELECT
    a.coid,
    a.coid_level_num,
    a.coid_name,
    a.coid_status_code,
    a.coid_type_code,
    a.coid_type_name,
    a.company_code,
    a.cons_facility_code,
    a.cons_facility_level_num,
    a.cons_facility_name,
    a.cons_facility_type_code,
    a.cons_facility_type_name,
    a.cons_income_category_code,
    a.corporate_code,
    a.corporate_level_num,
    a.corporate_name,
    a.corporate_type_code,
    a.corporate_type_name,
    a.division_code,
    a.division_level_num,
    a.division_name,
    a.division_type_code,
    a.division_type_name,
    a.financial_jv_code,
    a.fiscal_year_end_month,
    a.gl_consolidation_ind,
    a.gl_standard_chart_id,
    a.group_code,
    a.group_level_num,
    a.group_name,
    a.group_type_code,
    a.group_type_name,
    a.income_category,
    a.income_category_code,
    a.lob_code,
    a.lob_name,
    a.market_code,
    a.market_level_num,
    a.market_name,
    a.market_type_code,
    a.market_type_name,
    a.medicare_year_end_month,
    a.own_managed_code,
    a.own_percentage,
    a.sector_code,
    a.sector_level_num,
    a.sector_name,
    a.sector_type_code,
    a.sector_type_name,
    a.state_code,
    a.sub_lob_code,
    a.sub_lob_name,
    a.unit_num,
    a.data_source_code,
    a.pas_coid,
    a.pas_status,
    a.company_code_operations
  FROM
    {{ params.param_parallon_ra_base_views_dataset_name }}.fact_facility AS a
    CROSS JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b
  WHERE rtrim(a.coid) = rtrim(b.co_id)
   AND rtrim(a.company_code) = rtrim(b.company_code)
   AND rtrim(b.user_id) = rtrim(session_user())
;
