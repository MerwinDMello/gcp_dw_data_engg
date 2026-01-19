-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/junc_vendor_payor.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.junc_vendor_payor AS SELECT
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
    `hca-hin-dev-cur-parallon`.edwpbs.junc_vendor_payor
;
