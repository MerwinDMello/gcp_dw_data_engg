-- Translation time: 2024-09-06T08:53:37.054787Z
-- Translation job ID: 52a90b07-cafd-486a-a732-9cdfe62bb38f
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240906_0352/input/exp/j_im_pk_user.sql
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
                t4.im_domain_id,
                t1.pk_user_id_3_4 AS pk_user_id,
                t1.pk_database_instance_sid,
                DATE(t1.eff_from_date_time) AS pk_user_last_activity_date
              FROM
                `hca-hin-dev-cur-comp`.edwim_base_views.pk_login_information AS t1
                INNER JOIN `hca-hin-dev-cur-comp`.edwim_base_views.junc_pk_user_access_level AS t2 ON t1.pk_database_instance_sid = t2.pk_database_instance_sid
                 AND t1.pk_person_id = t2.pk_person_id
                 AND t2.pk_access_level_id = 3
                INNER JOIN `hca-hin-dev-cur-comp`.edwim_base_views.ref_pk_data_base_instance AS t3 ON t1.pk_database_instance_sid = t3.pk_database_instance_sid
                INNER JOIN `hca-hin-dev-cur-comp`.edwim_base_views.ref_im_domain AS t4 ON t4.application_system_id = 8
                 AND upper(rtrim(t3.pk_database_instance_code)) = upper(rtrim(t4.im_domain_name))
              WHERE length(trim(t1.pk_user_id_3_4)) = 7
               AND bqutil.fn.cw_regexp_instr_2(substr(trim(t1.pk_user_id_3_4), 4, 4), '[A-Za-z_]') = 0
               AND bqutil.fn.cw_regexp_instr_2(substr(trim(t1.pk_user_id_3_4), 1, 3), '[0-9_]') = 0
               AND DATE(t1.eff_from_date_time) >= date_sub(current_date('US/Central'), interval 365 DAY)
              QUALIFY row_number() OVER (PARTITION BY t1.pk_database_instance_sid, upper(pk_user_id) ORDER BY t1.eff_from_date_time DESC) = 1
          ) AS x
    ) AS a
;
