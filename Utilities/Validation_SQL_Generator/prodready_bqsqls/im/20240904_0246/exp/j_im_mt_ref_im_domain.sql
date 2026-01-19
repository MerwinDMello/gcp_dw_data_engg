-- Translation time: 2024-09-04T09:17:14.094843Z
-- Translation job ID: ac9a27eb-4692-4476-865e-b96823e9df9e
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240904_0246/input/exp/j_im_mt_ref_im_domain.sql
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