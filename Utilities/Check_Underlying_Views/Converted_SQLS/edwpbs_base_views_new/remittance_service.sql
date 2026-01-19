-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/remittance_service.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.remittance_service AS SELECT
    remittance_service.service_guid,
    remittance_service.claim_guid,
    remittance_service.audit_date,
    remittance_service.coid,
    remittance_service.company_code,
    remittance_service.delete_ind,
    remittance_service.delete_date,
    remittance_service.charge_amt,
    remittance_service.payment_amt,
    remittance_service.coinsurance_amt,
    remittance_service.deductible_amt,
    remittance_service.adjudicated_hcpcs_code,
    remittance_service.submitted_hcpcs_code,
    remittance_service.submitted_hcpcs_code_desc,
    remittance_service.payor_sent_revenue_code,
    remittance_service.adjudicated_hipps_code,
    remittance_service.submitted_hipps_code,
    remittance_service.apc_code,
    remittance_service.apc_amt,
    remittance_service.adjudicated_service_qty,
    remittance_service.submitted_service_qty,
    remittance_service.service_category_code,
    remittance_service.date_time_qualifier_code_1,
    remittance_service.service_date_1,
    remittance_service.date_time_qualifier_code_2,
    remittance_service.service_date_2,
    remittance_service.dw_last_update_date_time,
    remittance_service.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.remittance_service
;
