-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_ssd_budget.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.fact_ssd_budget
   OPTIONS(description='This table holds Budget data across multiple hierarchies of Shared Services.')
  AS SELECT
      a.account_sid,
      a.organization_sid,
      a.scenario_sid,
      a.time_sid,
      a.year_sid,
      a.department_sid,
      a.employee_sid,
      a.value_sid,
      a.coid,
      a.company_code,
      ROUND(a.budget_amount, 4, 'ROUND_HALF_EVEN') AS budget_amount,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_ssd_budget AS a
      INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
