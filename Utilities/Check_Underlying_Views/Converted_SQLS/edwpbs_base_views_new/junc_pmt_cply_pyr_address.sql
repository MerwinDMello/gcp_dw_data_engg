-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/junc_pmt_cply_pyr_address.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.junc_pmt_cply_pyr_address AS SELECT
    junc_pmt_cply_pyr_address.address_sid,
    junc_pmt_cply_pyr_address.iplan_id,
    junc_pmt_cply_pyr_address.coid,
    junc_pmt_cply_pyr_address.company_code,
    junc_pmt_cply_pyr_address.appeal_type_code,
    junc_pmt_cply_pyr_address.appeal_level_num,
    junc_pmt_cply_pyr_address.unit_num,
    junc_pmt_cply_pyr_address.auto_under_pmt_letter_ind,
    junc_pmt_cply_pyr_address.letter_type_code,
    junc_pmt_cply_pyr_address.ub_required_ind,
    junc_pmt_cply_pyr_address.form_required_ind,
    junc_pmt_cply_pyr_address.initial_subsequent_code,
    junc_pmt_cply_pyr_address.source_system_code,
    junc_pmt_cply_pyr_address.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.junc_pmt_cply_pyr_address
;
