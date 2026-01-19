-- Translation time: 2024-09-06T08:53:37.054787Z
-- Translation job ID: 52a90b07-cafd-486a-a732-9cdfe62bb38f
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240906_0352/input/exp/j_im_ad_member_group_hierarchy_archive.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', coalesce(count(*), 0)) AS source_string
FROM
  (SELECT t1.ad_archive_date AS ad_archive_date,
          t1.ad_archive_id AS ad_archive_id,
          t1.ad_domain_name AS ad_domain_name,
          t1.ad_member_distinguished_name AS ad_member_distinguished_name,
          t1.ad_group_distinguished_name AS ad_group_distinguished_name,
          t1.ad_parent_group_distinguished_name AS ad_parent_group_distinguished_name,
          t1.ad_user_name AS ad_user_name,
          t1.ad_extract_load_date_time AS ad_extract_load_date_time,
          t1.ad_group_name AS ad_group_name,
          t1.ad_hierarchy_level_id AS ad_hierarchy_level_id,
          t1.ad_parent_group_name AS ad_parent_group_name,
          t1.source_system_code AS source_system_code,
          t1.dw_last_update_date_time AS dw_last_update_date_time
   FROM
     (SELECT current_date('US/Central') AS ad_archive_date,
             row_number() OVER (
                                ORDER BY upper(t2.ad_user_name),
                                         t2.ad_hierarchy_level_id) AS ad_archive_id,
                               t2.ad_domain_name AS ad_domain_name,
                               t2.ad_member_distinguished_name AS ad_member_distinguished_name,
                               t2.ad_group_distinguished_name AS ad_group_distinguished_name,
                               t2.ad_parent_group_distinguished_name AS ad_parent_group_distinguished_name,
                               t2.ad_user_name AS ad_user_name,
                               t2.ad_extract_load_date_time AS ad_extract_load_date_time,
                               t2.ad_group_name AS ad_group_name,
                               t2.ad_hierarchy_level_id AS ad_hierarchy_level_id,
                               t2.ad_parent_group_name AS ad_parent_group_name,
                               t2.source_system_code AS source_system_code,
                               datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        (SELECT d.im_domain_name AS ad_domain_name,
                m.ad_member_distinguished_name AS ad_member_distinguished_name,
                g.ad_group_distinguished_name AS ad_group_distinguished_name,
                coalesce(p.ad_group_distinguished_name, '') AS ad_parent_group_distinguished_name,
                m.ad_user_name AS ad_user_name,
                h.ad_extract_load_date_time AS ad_extract_load_date_time,
                g.ad_group_name AS ad_group_name,
                h.ad_hierarchy_level_id AS ad_hierarchy_level_id,
                coalesce(p.ad_group_name, '') AS ad_parent_group_name,
                m.source_system_code AS source_system_code
         FROM `hca-hin-dev-cur-comp`.edwim.ad_member_group_hierarchy AS h
         INNER JOIN `hca-hin-dev-cur-comp`.edwim.ad_member_user AS m ON h.ad_member_id = m.ad_member_id
         INNER JOIN `hca-hin-dev-cur-comp`.edwim.ad_group_user AS g ON h.ad_group_id = g.ad_group_id
         LEFT OUTER JOIN `hca-hin-dev-cur-comp`.edwim.ad_group_user AS p ON h.ad_group_parent_id = p.ad_group_id
         INNER JOIN `hca-hin-dev-cur-comp`.edwim.ref_im_domain AS d ON m.im_domain_id = d.im_domain_id
         WHERE rtrim(coalesce(m.ad_user_name, '')) <> '' ) AS t2
      WHERE upper(rtrim(t2.ad_user_name)) IN
          (SELECT DISTINCT upper(rtrim(im_person_activity.im_person_user_id)) AS im_person_user_id
           FROM `hca-hin-dev-cur-comp`.edwim.im_person_activity
           WHERE im_person_activity.esaf_activity_date = current_date('US/Central')
             AND upper(rtrim(im_person_activity.source_system_code)) = 'M' ) ) AS t1) AS src