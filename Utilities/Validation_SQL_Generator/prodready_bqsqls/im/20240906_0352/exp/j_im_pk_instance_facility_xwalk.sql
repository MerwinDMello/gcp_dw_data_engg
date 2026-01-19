-- Translation time: 2024-09-06T08:53:37.054787Z
-- Translation job ID: 52a90b07-cafd-486a-a732-9cdfe62bb38f
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240906_0352/input/exp/j_im_pk_instance_facility_xwalk.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', coalesce(a.counts, 0)) AS source_string
FROM
  (SELECT count(*) AS counts
   FROM
     (SELECT DISTINCT t3.im_domain_id,
                      t2.company_code,
                      t2.coid
      FROM {{ params.param_im_auth_base_views_dataset_name }}.ref_pk_data_base_instance AS t1
      INNER JOIN {{ params.param_im_auth_base_views_dataset_name }}.pk_encounter AS t2 ON t1.pk_database_instance_sid = t2.pk_database_instance_sid
      INNER JOIN {{ params.param_im_base_views_dataset_name }}.ref_im_domain AS t3 ON upper(rtrim(t1.pk_database_instance_code)) = upper(rtrim(t3.im_domain_name))
      AND t3.application_system_id = 8) AS x) AS a