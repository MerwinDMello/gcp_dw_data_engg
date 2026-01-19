-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_ssd_budget.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_ssd_budget AS SELECT
    fact_ssd_budget.account_sid,
    fact_ssd_budget.organization_sid,
    fact_ssd_budget.scenario_sid,
    fact_ssd_budget.time_sid,
    fact_ssd_budget.year_sid,
    fact_ssd_budget.department_sid,
    fact_ssd_budget.employee_sid,
    fact_ssd_budget.value_sid,
    fact_ssd_budget.coid,
    fact_ssd_budget.company_code,
    fact_ssd_budget.budget_amount,
    fact_ssd_budget.source_system_code,
    fact_ssd_budget.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.fact_ssd_budget
;
