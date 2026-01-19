-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/remittance_service.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.remittance_service AS SELECT
    a.service_guid,
    a.claim_guid,
    a.audit_date,
    a.coid,
    a.company_code,
    a.delete_ind,
    a.delete_date,
    a.charge_amt,
    a.payment_amt,
    a.coinsurance_amt,
    a.deductible_amt,
    a.adjudicated_hcpcs_code,
    a.submitted_hcpcs_code,
    a.submitted_hcpcs_code_desc,
    a.payor_sent_revenue_code,
    a.adjudicated_hipps_code,
    a.submitted_hipps_code,
    a.apc_code,
    a.apc_amt,
    a.adjudicated_service_qty,
    a.submitted_service_qty,
    a.service_category_code,
    a.date_time_qualifier_code_1,
    a.service_date_1,
    a.date_time_qualifier_code_2,
    a.service_date_2,
    a.dw_last_update_date_time,
    a.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.remittance_service AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
