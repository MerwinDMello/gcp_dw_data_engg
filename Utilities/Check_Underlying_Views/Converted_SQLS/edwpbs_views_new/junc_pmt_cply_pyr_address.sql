-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/junc_pmt_cply_pyr_address.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.junc_pmt_cply_pyr_address AS SELECT
    a.address_sid,
    a.iplan_id,
    a.coid,
    a.company_code,
    a.appeal_type_code,
    a.appeal_level_num,
    a.unit_num,
    a.auto_under_pmt_letter_ind,
    a.letter_type_code,
    a.ub_required_ind,
    a.form_required_ind,
    a.initial_subsequent_code,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.junc_pmt_cply_pyr_address AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
