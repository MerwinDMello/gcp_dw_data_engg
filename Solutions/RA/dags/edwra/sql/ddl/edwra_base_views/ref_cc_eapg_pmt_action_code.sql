-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/ref_cc_eapg_pmt_action_code.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_eapg_pmt_action_code
   OPTIONS(description='Reference table for descriptions related to enhanced all payer grouping payment action codes.  This table will contain static values.')
  AS SELECT
      ref_cc_eapg_pmt_action_code.eapg_payment_action_code,
      ref_cc_eapg_pmt_action_code.eapg_payment_action_code_desc,
      ref_cc_eapg_pmt_action_code.source_system_code,
      ref_cc_eapg_pmt_action_code.dw_last_update_date_time
    FROM
      {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_eapg_pmt_action_code
  ;
