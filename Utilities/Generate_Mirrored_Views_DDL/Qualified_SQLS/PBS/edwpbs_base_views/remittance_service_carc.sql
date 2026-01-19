-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/remittance_service_carc.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.remittance_service_carc
   OPTIONS(description='This is the information related to Service level adjustments associated with the service.')
  AS SELECT
      remittance_service_carc.service_guid,
      remittance_service_carc.adj_group_code,
      remittance_service_carc.carc_code,
      remittance_service_carc.audit_date,
      remittance_service_carc.delete_ind,
      remittance_service_carc.delete_date,
      remittance_service_carc.coid,
      remittance_service_carc.company_code,
      ROUND(remittance_service_carc.adj_amt, 3, 'ROUND_HALF_EVEN') AS adj_amt,
      remittance_service_carc.adj_qty,
      remittance_service_carc.adj_category,
      remittance_service_carc.cc_adj_group_code,
      remittance_service_carc.dw_last_update_date_time,
      remittance_service_carc.source_system_code
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.remittance_service_carc
  ;
