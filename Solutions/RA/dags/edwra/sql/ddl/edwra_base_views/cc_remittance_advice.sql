-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/cc_remittance_advice.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.cc_remittance_advice
   OPTIONS(description='This table contains advice related information received on a remittance record.')
  AS SELECT
      cc_remittance_advice.company_code,
      cc_remittance_advice.coid,
      cc_remittance_advice.payor_dw_id,
      cc_remittance_advice.remittance_advice_num,
      cc_remittance_advice.remittance_header_id,
      cc_remittance_advice.unit_num,
      cc_remittance_advice.iplan_id,
      cc_remittance_advice.payment_date,
      cc_remittance_advice.remittance_date,
      cc_remittance_advice.remittance_amt,
      cc_remittance_advice.check_num,
      cc_remittance_advice.icn_num,
      cc_remittance_advice.group_control_num,
      cc_remittance_advice.create_date_time,
      cc_remittance_advice.dw_last_update_date_time,
      cc_remittance_advice.source_system_code
    FROM
      {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance_advice
  ;
