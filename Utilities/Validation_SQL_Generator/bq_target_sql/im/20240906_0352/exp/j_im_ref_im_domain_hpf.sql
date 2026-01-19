-- Translation time: 2024-09-06T08:53:37.054787Z
-- Translation job ID: 52a90b07-cafd-486a-a732-9cdfe62bb38f
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240906_0352/input/exp/j_im_ref_im_domain_hpf.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', coalesce(a.counts, 0)) AS source_string
  FROM
    (
      SELECT
          count(DISTINCT upper(document_work_flow_instance.instance_connection_string)) AS counts
        FROM
          `hca-hin-dev-cur-comp`.edwdw_base_views.document_work_flow_instance
    ) AS a
;
