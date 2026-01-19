-- Translation time: 2024-09-06T08:53:37.054787Z
-- Translation job ID: 52a90b07-cafd-486a-a732-9cdfe62bb38f
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240906_0352/input/exp/j_im_mt_instance_facility_xwalk.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', coalesce(a.counts, 0)) AS source_string
FROM
  (SELECT count(*) AS counts
   FROM
     (SELECT x.im_domain_id,
             max(x.company_code) AS company_code,
             max(x.coid) AS coid
      FROM
        (SELECT DISTINCT t3.im_domain_id,
                         t1.company_code,
                         t1.coid,
                         CASE
                             WHEN rtrim(t1.coid) = '31768' THEN 6
                             ELSE 5
                         END AS appl_system_id
         FROM `hca-hin-dev-cur-comp`.auth_base_views.clinical_facility AS t1
         INNER JOIN `hca-hin-dev-cur-comp`.edwim_base_views.ref_im_domain AS t3 ON rtrim(t1.network_mnemonic_cs) = rtrim(t3.im_domain_name)
         AND t3.application_system_id = CASE
                                            WHEN rtrim(t1.coid) = '31768' THEN 6
                                            ELSE 5
                                        END) AS x
      GROUP BY 1,
               upper(x.company_code),
               upper(x.coid)) AS x) AS a