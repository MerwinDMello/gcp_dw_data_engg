-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_anesthesia_type_cdm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_anesthesia_type_cdm AS SELECT
    a.company_code,
    a.coid,
    a.anesthesia_type_mnemonic_cs,
    a.anesthesia_type_mnemonic,
    a.anesthesia_type_desc,
    a.anesthesia_type_active_ind,
    a.nomenclature_code,
    a.facility_mnemonic_cs,
    a.network_mnemonic_cs,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwcdm_base_views.ref_anesthesia_type AS a
;
