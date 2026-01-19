-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/edw_ad_user_group_info_ac.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.edw_ad_user_group_info_ac AS SELECT
    edw_ad_user_group_info_ac.collectdatetime,
    edw_ad_user_group_info_ac.target_system,
    edw_ad_user_group_info_ac.user_domain,
    edw_ad_user_group_info_ac.user_id,
    edw_ad_user_group_info_ac.group_type,
    edw_ad_user_group_info_ac.group_unit_num,
    edw_ad_user_group_info_ac.group_text,
    edw_ad_user_group_info_ac.user_created,
    edw_ad_user_group_info_ac.user_modified,
    edw_ad_user_group_info_ac.change_flag,
    edw_ad_user_group_info_ac.transaction_type,
    edw_ad_user_group_info_ac.true_level_ind,
    edw_ad_user_group_info_ac.coid,
    edw_ad_user_group_info_ac.summary_ind1,
    edw_ad_user_group_info_ac.summary_ind2,
    edw_ad_user_group_info_ac.summary_ind3,
    edw_ad_user_group_info_ac.summary_ind4,
    edw_ad_user_group_info_ac.summary_ind5,
    edw_ad_user_group_info_ac.mismatch_flag,
    edw_ad_user_group_info_ac.adhoc_id_flag,
    edw_ad_user_group_info_ac.bi_id_flag,
    edw_ad_user_group_info_ac.request_completed_flag,
    edw_ad_user_group_info_ac.reason_ind,
    edw_ad_user_group_info_ac.seq_num
  FROM
    `hca-hin-dev-cur-parallon`.dbadmin_views.edw_ad_user_group_info_ac
UNION ALL
SELECT
    date_sub(current_date('US/Central'), interval 1 DAY) AS collectdatetime,
    edw_ad_user_group_info.target_system AS target_system,
    edw_ad_user_group_info.user_domain AS user_domain,
    edw_ad_user_group_info.user_id AS user_id,
    edw_ad_user_group_info.group_type AS group_type,
    edw_ad_user_group_info.group_unit_num AS group_unit_num,
    edw_ad_user_group_info.group_text AS group_text,
    edw_ad_user_group_info.user_created AS user_created,
    edw_ad_user_group_info.user_modified AS user_modified,
    edw_ad_user_group_info.change_flag AS change_flag,
    edw_ad_user_group_info.transaction_type AS transaction_type,
    edw_ad_user_group_info.true_level_ind AS true_level_ind,
    edw_ad_user_group_info.coid AS coid,
    edw_ad_user_group_info.summary_ind1 AS summary_ind1,
    edw_ad_user_group_info.summary_ind2 AS summary_ind2,
    edw_ad_user_group_info.summary_ind3 AS summary_ind3,
    edw_ad_user_group_info.summary_ind4 AS summary_ind4,
    edw_ad_user_group_info.summary_ind5 AS summary_ind5,
    edw_ad_user_group_info.mismatch_flag AS mismatch_flag,
    edw_ad_user_group_info.adhoc_id_flag AS adhoc_id_flag,
    edw_ad_user_group_info.bi_id_flag AS bi_id_flag,
    edw_ad_user_group_info.request_completed_flag AS request_completed_flag,
    edw_ad_user_group_info.reason_ind AS reason_ind,
    edw_ad_user_group_info.seq_num AS seq_num
  FROM
    `hca-hin-dev-cur-parallon`.dbadmin_views.edw_ad_user_group_info
;
