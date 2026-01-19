-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_ssd_budget.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_ssd_budget
   OPTIONS(description='This table holds Budget data across multiple hierarchies of Shared Services.')
  AS SELECT
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
      ROUND(fact_ssd_budget.budget_amount, 4, 'ROUND_HALF_EVEN') AS budget_amount,
      fact_ssd_budget.source_system_code,
      fact_ssd_budget.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs.fact_ssd_budget
  ;
