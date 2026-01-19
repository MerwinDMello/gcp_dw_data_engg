-- Translation time: 2024-09-04T09:17:14.094843Z
-- Translation job ID: ac9a27eb-4692-4476-865e-b96823e9df9e
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240904_0246/input/exp/j_im_cactus_provider_archive.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', coalesce(f.counts, 0)) AS source_string
FROM
  (SELECT count(*) AS counts
   FROM
     (SELECT t2.im_domain_id,
             t1.cactus_provider_user_id,
             t2.esaf_activity_date,
             t1.cactus_provider_src_sys_key,
             t1.cactus_provider_npi,
             t1.fac_asgn_stts_sid,
             t1.fac_asgn_stts_src_sys_key,
             t1.cactus_provider_cat_sid,
             t1.cactus_provider_cat_src_sys_key,
             t1.cactus_provider_first_name,
             t1.cactus_provider_last_name,
             t1.cactus_provider_middle_name,
             t1.cactus_provider_activity_exempt_sw,
             t1.source_system_code,
             t1.dw_last_update_date_time
      FROM `hca-hin-dev-cur-comp`.edwim_base_views.cactus_provider AS t1
      INNER JOIN `hca-hin-dev-cur-comp`.edwim_base_views.im_person_activity AS t2 ON upper(rtrim(t1.cactus_provider_user_id)) = upper(rtrim(t2.im_person_user_id))
      AND t1.im_domain_id = t2.im_domain_id QUALIFY row_number() OVER (PARTITION BY t2.im_domain_id,
                                                                                    upper(t1.cactus_provider_user_id)
                                                                       ORDER BY t1.fac_asgn_stts_sid DESC) = 1) AS a) AS f