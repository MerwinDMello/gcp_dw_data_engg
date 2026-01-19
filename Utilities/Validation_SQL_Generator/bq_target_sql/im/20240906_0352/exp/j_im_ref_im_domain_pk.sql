-- Translation time: 2024-09-06T08:53:37.054787Z
-- Translation job ID: 52a90b07-cafd-486a-a732-9cdfe62bb38f
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240906_0352/input/exp/j_im_ref_im_domain_pk.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', coalesce(a.counts, 0)) AS source_string
  FROM
    (
      SELECT
          count(DISTINCT ref_pk_data_base_instance.pk_database_instance_code) AS counts
        FROM
          `hca-hin-dev-cur-comp`.edwim_base_views.ref_pk_data_base_instance
    ) AS a
;
