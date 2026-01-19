-- Translation time: 2024-09-04T09:17:14.094843Z
-- Translation job ID: ac9a27eb-4692-4476-865e-b96823e9df9e
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240904_0246/input/exp/j_im_hpf_instance_facility_xwalk.sql
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