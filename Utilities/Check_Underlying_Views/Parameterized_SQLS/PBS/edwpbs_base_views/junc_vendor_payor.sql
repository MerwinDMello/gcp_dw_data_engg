-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/junc_vendor_payor.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.junc_vendor_payor AS SELECT
    junc_vendor_payor.coid,
    junc_vendor_payor.hosp_no,
    junc_vendor_payor.vendor_key,
    junc_vendor_payor.ins_plan,
    junc_vendor_payor.ins_plan_desc,
    junc_vendor_payor.payor_name,
    junc_vendor_payor.major_payor_desc,
    junc_vendor_payor.financial_class_code,
    junc_vendor_payor.dw_last_update_date_time
  FROM
    {{ params.param_pbs_core_dataset_name }}.junc_vendor_payor
;
