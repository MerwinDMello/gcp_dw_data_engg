-- Translation time: 2024-09-06T08:53:37.054787Z
-- Translation job ID: 52a90b07-cafd-486a-a732-9cdfe62bb38f
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240906_0352/input/exp/j_im_meditech_user_archive.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', coalesce(count(*), 0)) AS source_string
FROM
  (SELECT t2.im_domain_id,
          t1.mt_user_id,
          t2.esaf_activity_date,
          t1.national_provider_id,
          t1.facility_mnemonic_cs,
          t1.mt_user_full_name,
          t1.mt_user_mnemonic_cs,
          t1.mt_user_page_1_provider_type_desc,
          t1.mt_user_page_2_provider_type_desc,
          t1.mt_user_exempt_sw,
          t1.mt_user_active_ind,
          t1.mt_user_mis_user_mnemonic,
          t1.mt_linked_user_code,
          t1.mt_user_last_activity_date,
          t1.source_system_code,
          t1.dw_last_update_date_time
   FROM {{ params.param_im_base_views_dataset_name }}.meditech_user AS t1
   INNER JOIN {{ params.param_im_base_views_dataset_name }}.im_person_activity AS t2 ON upper(rtrim(t1.mt_user_id)) = upper(rtrim(t2.im_person_user_id))
   AND t1.im_domain_id = t2.im_domain_id
   UNION DISTINCT SELECT t3.im_domain_id,
                         t1.mt_user_id,
                         t3.esaf_activity_date,
                         t1.national_provider_id,
                         t1.facility_mnemonic_cs,
                         t1.mt_user_full_name,
                         t1.mt_user_mnemonic_cs,
                         t1.mt_user_page_1_provider_type_desc,
                         t1.mt_user_page_2_provider_type_desc,
                         t1.mt_user_exempt_sw,
                         t1.mt_user_active_ind,
                         t1.mt_user_mis_user_mnemonic,
                         t1.mt_linked_user_code,
                         t1.mt_user_last_activity_date,
                         t1.source_system_code,
                         t1.dw_last_update_date_time
   FROM {{ params.param_im_base_views_dataset_name }}.meditech_user AS t1
   INNER JOIN
     (SELECT DISTINCT h2.hpf_domain_id,
                      h2.mt_domain_id,
                      h1.mt_user_id
      FROM {{ params.param_im_base_views_dataset_name }}.meditech_user AS h1
      INNER JOIN {{ params.param_im_base_views_dataset_name }}.platform_domain_xwalk AS h2 ON h1.im_domain_id = h2.mt_domain_id) AS t2 ON t1.im_domain_id = t2.mt_domain_id
   AND upper(rtrim(t1.mt_user_id)) = upper(rtrim(t2.mt_user_id))
   INNER JOIN {{ params.param_im_base_views_dataset_name }}.im_person_activity AS t3 ON upper(rtrim(t2.mt_user_id)) = upper(rtrim(t3.im_person_user_id))
   AND t2.hpf_domain_id = t3.im_domain_id QUALIFY row_number() OVER (PARTITION BY t3.im_domain_id,
                                                                                  upper(t1.mt_user_id),
                                                                                  t1.facility_mnemonic_cs
                                                                     ORDER BY t1.mt_user_exempt_sw,
                                                                              t1.mt_user_last_activity_date DESC) = 1) AS a