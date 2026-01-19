-- Translation time: 2024-09-06T08:53:37.054787Z
-- Translation job ID: 52a90b07-cafd-486a-a732-9cdfe62bb38f
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240906_0352/input/exp/j_im_pk_user_archive.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', coalesce(a.counts, 0)) AS source_string
FROM
  (SELECT count(*) AS counts
   FROM
     (SELECT t2.im_domain_id,
             t1.pk_user_id,
             t2.esaf_activity_date,
             t1.pk_database_instance_id,
             t1.pk_user_last_activity_date,
             t1.source_system_code,
             t1.dw_last_update_date_time
      FROM `hca-hin-dev-cur-comp`.edwim_base_views.pk_user AS t1
      INNER JOIN `hca-hin-dev-cur-comp`.edwim_base_views.im_person_activity AS t2 ON upper(rtrim(t1.pk_user_id)) = upper(rtrim(t2.im_person_user_id)) QUALIFY row_number() OVER (PARTITION BY t2.im_domain_id,
                                                                                                                                                                                              upper(t1.pk_user_id)
                                                                                                                                                                                 ORDER BY t1.pk_user_last_activity_date DESC) = 1) AS x) AS a