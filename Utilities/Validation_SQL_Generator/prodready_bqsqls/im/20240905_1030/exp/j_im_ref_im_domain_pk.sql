-- Translation time: 2024-09-05T15:31:41.992Z
-- Translation job ID: 2a0106aa-7f86-44a1-bb13-2e1417e5dce4
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240905_1030/input/exp/j_im_ref_im_domain_pk.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', coalesce(a.counts, 0)) AS source_string
FROM
  (SELECT count(DISTINCT upper(ref_pk_data_base_instance.pk_database_instance_code)) AS counts
   FROM {{ params.param_im_auth_base_views_dataset_name }}.ref_pk_data_base_instance) AS a