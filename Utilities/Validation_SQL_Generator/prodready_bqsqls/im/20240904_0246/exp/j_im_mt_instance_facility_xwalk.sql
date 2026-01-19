-- Translation time: 2024-09-04T09:17:14.094843Z
-- Translation job ID: ac9a27eb-4692-4476-865e-b96823e9df9e
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240904_0246/input/exp/j_im_mt_instance_facility_xwalk.sql
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
         FROM {{ params.param_im_auth_base_views_dataset_name }}.clinical_facility AS t1
         INNER JOIN {{ params.param_im_base_views_dataset_name }}.ref_im_domain AS t3 ON rtrim(t1.network_mnemonic_cs) = rtrim(t3.im_domain_name)
         AND t3.application_system_id = CASE
                                            WHEN rtrim(t1.coid) = '31768' THEN 6
                                            ELSE 5
                                        END) AS x
      GROUP BY 1,
               upper(x.company_code),
               upper(x.coid)) AS x) AS a