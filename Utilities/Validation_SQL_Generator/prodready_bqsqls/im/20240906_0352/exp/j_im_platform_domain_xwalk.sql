-- Translation time: 2024-09-06T08:53:37.054787Z
-- Translation job ID: 52a90b07-cafd-486a-a732-9cdfe62bb38f
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240906_0352/input/exp/j_im_platform_domain_xwalk.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', coalesce(a.counts, 0)) AS source_string
FROM
  (SELECT count(*) AS counts
   FROM
     (SELECT DISTINCT t1.im_domain_id,
                      coalesce(t2.im_domain_id, 0) AS mt_domain_id,
                      coalesce(t3.im_domain_id, 0) AS pk_domain_id
      FROM {{ params.param_im_stage_dataset_name }}.hpf_instance_facility_xwalk AS t1
      LEFT OUTER JOIN {{ params.param_im_stage_dataset_name }}.mt_instance_facility_xwalk AS t2 ON upper(rtrim(t1.company_code)) = upper(rtrim(t2.company_code))
      AND upper(rtrim(t1.coid)) = upper(rtrim(t2.coid))
      LEFT OUTER JOIN {{ params.param_im_stage_dataset_name }}.pk_instance_facility_xwalk AS t3 ON upper(rtrim(t1.company_code)) = upper(rtrim(t3.company_code))
      AND upper(rtrim(t1.coid)) = upper(rtrim(t3.coid))) AS x) AS a