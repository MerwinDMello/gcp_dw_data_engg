-- Translation time: 2024-09-05T15:31:41.992Z
-- Translation job ID: 2a0106aa-7f86-44a1-bb13-2e1417e5dce4
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240905_1030/input/exp/j_im_platform_domain_xwalk.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', coalesce(a.counts, 0)) AS source_string
FROM
  (SELECT count(*) AS counts
   FROM
     (SELECT DISTINCT t1.im_domain_id,
                      coalesce(t2.im_domain_id, 0) AS mt_domain_id,
                      coalesce(t3.im_domain_id, 0) AS pk_domain_id
      FROM `hca-hin-dev-cur-comp`.edwim_staging.hpf_instance_facility_xwalk AS t1
      LEFT OUTER JOIN `hca-hin-dev-cur-comp`.edwim_staging.mt_instance_facility_xwalk AS t2 ON upper(rtrim(t1.company_code)) = upper(rtrim(t2.company_code))
      AND upper(rtrim(t1.coid)) = upper(rtrim(t2.coid))
      LEFT OUTER JOIN `hca-hin-dev-cur-comp`.edwim_staging.pk_instance_facility_xwalk AS t3 ON upper(rtrim(t1.company_code)) = upper(rtrim(t3.company_code))
      AND upper(rtrim(t1.coid)) = upper(rtrim(t3.coid))) AS x) AS a