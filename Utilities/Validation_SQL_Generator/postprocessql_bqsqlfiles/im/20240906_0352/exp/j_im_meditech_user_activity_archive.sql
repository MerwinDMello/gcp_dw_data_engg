-- Translation time: 2024-09-06T08:53:37.054787Z
-- Translation job ID: 52a90b07-cafd-486a-a732-9cdfe62bb38f
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240906_0352/input/exp/j_im_meditech_user_activity_archive.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', coalesce(count(*), 0)) AS source_string
FROM
  (SELECT t2.im_domain_id,
          t1.mt_user_id,
          t2.esaf_activity_date,
          t1.mt_user_last_activity_date,
          t1.mt_user_activity_sw,
          t1.mt_excluded_user_sw,
          t1.mt_staff_pm_user_sw,
          t1.mt_pcp_user_sw,
          t1.mt_alias_exempt_sw,
          t1.mt_linked_user_sw,
          t1.mt_open_deficiency_sw,
          t1.dw_last_update_date_time
   FROM `hca-hin-dev-cur-comp`.edwim_base_views.meditech_user_activity AS t1
   INNER JOIN `hca-hin-dev-cur-comp`.edwim_base_views.im_person_activity AS t2 ON upper(rtrim(t1.mt_user_id)) = upper(rtrim(t2.im_person_user_id))
   AND t1.im_domain_id = t2.im_domain_id QUALIFY row_number() OVER (PARTITION BY t2.im_domain_id,
                                                                                 upper(t1.mt_user_id)
                                                                    ORDER BY t1.mt_user_last_activity_date DESC) = 1) AS a