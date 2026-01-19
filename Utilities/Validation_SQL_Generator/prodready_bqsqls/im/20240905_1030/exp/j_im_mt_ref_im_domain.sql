-- Translation time: 2024-09-05T15:31:41.992Z
-- Translation job ID: 2a0106aa-7f86-44a1-bb13-2e1417e5dce4
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240905_1030/input/exp/j_im_mt_ref_im_domain.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', coalesce(a.counts, 0)) AS source_string
FROM
  (SELECT count(*) AS counts
   FROM
     (SELECT count(*) AS counts
      FROM {{ params.param_im_stage_dataset_name }}.stg_ref_im_domain
      GROUP BY stg_ref_im_domain.application_system_id,
               upper(stg_ref_im_domain.im_domain_name),
               upper(stg_ref_im_domain.im_domain_desc),
               upper(stg_ref_im_domain.source_system_code),
               stg_ref_im_domain.dw_last_update_date_time) AS b) AS a