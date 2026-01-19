-- Translation time: 2024-09-06T08:53:37.054787Z
-- Translation job ID: 52a90b07-cafd-486a-a732-9cdfe62bb38f
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240906_0352/input/exp/j_im_hpf_instance_facility_xwalk.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', coalesce(a.counts, 0)) AS source_string
FROM
  (SELECT count(*) AS counts
   FROM
     (SELECT DISTINCT t3.im_domain_id,
                      t2.company_code,
                      t2.coid
      FROM `hca-hin-dev-cur-comp`.auth_base_views.document_work_flow_instance AS t1
      INNER JOIN `hca-hin-dev-cur-comp`.auth_base_views.deficiency_audit AS t2 ON t1.instance_dw_id = t2.instance_dw_id
      INNER JOIN `hca-hin-dev-cur-comp`.edwim_base_views.ref_im_domain AS t3 ON upper(rtrim(t1.instance_connection_string)) = upper(rtrim(t3.im_domain_name))
      AND t3.application_system_id = 3) AS x) AS a