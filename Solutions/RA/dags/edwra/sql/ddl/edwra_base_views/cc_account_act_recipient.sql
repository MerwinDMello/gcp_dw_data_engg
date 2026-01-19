-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/cc_account_act_recipient.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.cc_account_act_recipient AS SELECT
    cc_account_act_recipient.patient_dw_id,
    cc_account_act_recipient.activity_id,
    cc_account_act_recipient.activity_recipient_id,
    cc_account_act_recipient.company_code,
    cc_account_act_recipient.coid,
    cc_account_act_recipient.unit_num,
    cc_account_act_recipient.pat_acct_num,
    cc_account_act_recipient.recipient_login_userid,
    cc_account_act_recipient.dw_last_update_date_time,
    cc_account_act_recipient.source_system_code
  FROM
    {{ params.param_parallon_ra_core_dataset_name }}.cc_account_act_recipient
;
