-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/cc_remittance_advice.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.cc_remittance_advice
   OPTIONS(description='This table contains advice related information received on a remittance record.')
  AS SELECT
      a.company_code,
      a.coid,
      a.payor_dw_id,
      a.remittance_advice_num,
      a.remittance_header_id,
      a.unit_num,
      a.iplan_id,
      a.payment_date,
      a.remittance_date,
      a.remittance_amt,
      a.check_num,
      a.icn_num,
      a.group_control_num,
      a.create_date_time,
      a.dw_last_update_date_time,
      a.source_system_code
    FROM
      {{ params.param_parallon_ra_base_views_dataset_name }}.cc_remittance_advice AS a
      INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b ON rtrim(a.company_code) = rtrim(b.company_code)
       AND rtrim(a.coid) = rtrim(b.co_id)
       AND rtrim(b.user_id) = rtrim(session_user())
  ;
