-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/comast_nbt_supplement.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.comast_nbt_supplement AS SELECT
    comast_nbt_supplement.coid,
    comast_nbt_supplement.coid_status_id,
    comast_nbt_supplement.company_code,
    comast_nbt_supplement.vp_num,
    comast_nbt_supplement.group_vice_president_name,
    comast_nbt_supplement.hospital_coid,
    comast_nbt_supplement.hospital_unit_num,
    comast_nbt_supplement.hospital_name,
    comast_nbt_supplement.joint_venture_ind,
    comast_nbt_supplement.account_location_code,
    comast_nbt_supplement.consolidator_ind,
    comast_nbt_supplement.consolidator_id,
    comast_nbt_supplement.tax_id,
    comast_nbt_supplement.coid_start_date,
    comast_nbt_supplement.hbp_program_type_id,
    comast_nbt_supplement.hbp_program_start_date,
    comast_nbt_supplement.ar_system_code,
    comast_nbt_supplement.ar_system_name,
    comast_nbt_supplement.same_store_ind,
    comast_nbt_supplement.lob_code,
    comast_nbt_supplement.lob_sub_code,
    comast_nbt_supplement.total_operation_ind,
    comast_nbt_supplement.data_source_code,
    comast_nbt_supplement.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwps_base_views.comast_nbt_supplement
;
