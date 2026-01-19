-- Translation time: 2024-09-04T09:17:14.094843Z
-- Translation job ID: ac9a27eb-4692-4476-865e-b96823e9df9e
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240904_0246/input/exp/j_im_mt_cl_hcp.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', coalesce(count(*), 0)) AS source_string
FROM
  (SELECT t1.company_code AS company_code,
          t1.coid AS coid,
          t1.hcp_mnemonic_cs AS hcp_mnemonic_cs,
          coalesce(t1.hcp_mis_user_mnemonic, '') AS hcp_mis_user_mnemonic,
          t1.network_mnemonic_cs AS network_mnemonic_cs,
          t1.national_provider_id AS national_provider_id,
          hcp_user_id_3_4 AS hcp_user_id_3_4,
          t1.hcp_full_name AS hcp_full_name,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS time_stamp
   FROM `hca-hin-dev-cur-comp`.auth_base_views.clinical_health_care_provider AS t1
   LEFT OUTER JOIN `hca-hin-dev-cur-comp`.edwim_staging.mt_cl_hcp_cdm AS t2 ON t1.hcp_dw_id = t2.hcp_dw_id
   LEFT OUTER JOIN `hca-hin-dev-cur-comp`.edwim_staging.mt_cl_hcp_vc AS t3 ON t1.hcp_dw_id = t3.hcp_dw_id
   CROSS JOIN UNNEST(ARRAY[ coalesce(t2.hcp_user_id, t3.hcp_user_id) ]) AS hcp_user_id_3_4
   WHERE NOT upper(rtrim(t1.hcp_mnemonic_cs)) = 'UNDEFINED'
     AND NOT hcp_user_id_3_4 IS NULL
     AND NOT rtrim(hcp_user_id_3_4) = '' QUALIFY row_number() OVER (PARTITION BY t1.hcp_dw_id,
                                                                                 upper(hcp_mnemonic_cs)
                                                                    ORDER BY upper(t1.hcp_mis_user_mnemonic) DESC, upper(hcp_user_id_3_4) DESC) = 1 ) AS a