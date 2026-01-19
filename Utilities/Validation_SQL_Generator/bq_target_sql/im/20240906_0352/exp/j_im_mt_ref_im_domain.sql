-- Translation time: 2024-09-06T08:53:37.054787Z
-- Translation job ID: 52a90b07-cafd-486a-a732-9cdfe62bb38f
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240906_0352/input/exp/j_im_mt_ref_im_domain.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', coalesce(a.counts, 0)) AS source_string
  FROM
    (
      SELECT
          count(*) AS counts
        FROM
          (
            SELECT
                count(*) AS counts
              FROM
                `hca-hin-dev-cur-comp`.edwim_staging.stg_ref_im_domain
              GROUP BY stg_ref_im_domain.application_system_id, upper(stg_ref_im_domain.im_domain_name), upper(stg_ref_im_domain.im_domain_desc), upper(stg_ref_im_domain.source_system_code), stg_ref_im_domain.dw_last_update_date_time
          ) AS b
    ) AS a
;
