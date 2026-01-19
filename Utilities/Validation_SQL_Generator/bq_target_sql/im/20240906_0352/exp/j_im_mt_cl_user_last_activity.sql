-- Translation time: 2024-09-06T08:53:37.054787Z
-- Translation job ID: 52a90b07-cafd-486a-a732-9cdfe62bb38f
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240906_0352/input/exp/j_im_mt_cl_user_last_activity.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', coalesce(count(*), 0)) AS source_string
  FROM
    (
      SELECT
          t1.company_code AS company_code,
          t1.coid AS coid,
          t1.mt_user_mnemonic_cs AS mt_user_mnemonic_cs,
          t1.network_mnemonic_cs AS network_mnemonic_cs,
          t1.mt_user_last_activity_date AS mt_user_last_activity_date,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS time_stamp
        FROM
          (
            SELECT
                mt_cl_user_activity.company_code AS company_code,
                mt_cl_user_activity.coid AS coid,
                mt_cl_user_activity.mt_user_mnemonic_cs AS mt_user_mnemonic_cs,
                mt_cl_user_activity.network_mnemonic_cs AS network_mnemonic_cs,
                mt_cl_user_activity.mt_user_last_activity_date AS mt_user_last_activity_date
              FROM
                `hca-hin-dev-cur-comp`.edwim_staging.mt_cl_user_activity
            UNION ALL
            SELECT
                mt_cl_user_activity_hist.company_code AS company_code,
                mt_cl_user_activity_hist.coid AS coid,
                mt_cl_user_activity_hist.mt_user_mnemonic_cs AS mt_user_mnemonic_cs,
                mt_cl_user_activity_hist.network_mnemonic_cs AS network_mnemonic_cs,
                mt_cl_user_activity_hist.mt_user_last_activity_date AS mt_user_last_activity_date
              FROM
                `hca-hin-dev-cur-comp`.edwim_staging.mt_cl_user_activity_hist
          ) AS t1
        QUALIFY row_number() OVER (PARTITION BY network_mnemonic_cs, mt_user_mnemonic_cs ORDER BY mt_user_last_activity_date DESC) = 1
    ) AS a
;
