-- Translation time: 2024-09-05T15:31:41.992Z
-- Translation job ID: 2a0106aa-7f86-44a1-bb13-2e1417e5dce4
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240905_1030/input/exp/j_im_mt_instance_facility_xwalk.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', coalesce(a.counts, 0)) AS source_string
  FROM
    (
      SELECT
          count(*) AS counts
        FROM
          (
            SELECT
                x.im_domain_id,
                max(x.company_code) AS company_code,
                max(x.coid) AS coid
              FROM
                (
                  SELECT DISTINCT
                      t3.im_domain_id,
                      t1.company_code,
                      t1.coid,
                      CASE
                        WHEN rtrim(t1.coid) = '31768' THEN 6
                        ELSE 5
                      END AS appl_system_id
                    FROM
                      `hca-hin-dev-cur-comp`.edw_pub_views.clinical_facility AS t1
                      INNER JOIN `hca-hin-dev-cur-comp`.edwim_base_views.ref_im_domain AS t3 ON rtrim(t1.network_mnemonic_cs) = rtrim(t3.im_domain_name)
                       AND t3.application_system_id = CASE
                        WHEN rtrim(t1.coid) = '31768' THEN 6
                        ELSE 5
                      END
                ) AS x
              GROUP BY 1, upper(x.company_code), upper(x.coid)
          ) AS x
    ) AS a
;
