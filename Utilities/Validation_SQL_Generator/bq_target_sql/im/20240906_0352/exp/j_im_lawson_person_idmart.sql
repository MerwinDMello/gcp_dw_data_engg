-- Translation time: 2024-09-06T08:53:37.054787Z
-- Translation job ID: 52a90b07-cafd-486a-a732-9cdfe62bb38f
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240906_0352/input/exp/j_im_lawson_person_idmart.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', count(*)) AS source_string
  FROM
    (
      SELECT
          m2.im_domain_id,
          m2.lawson_person_user_id,
          m2.lawson_empl_status_code,
          m2.lawson_person_supv_user_id,
          m2.lawson_person_empl_num,
          m2.lawson_person_action_name,
          m2.lawson_person_hire_date,
          m2.lawson_person_posn_start_date,
          m2.lawson_person_termn_date,
          m2.lawson_person_last_name,
          m2.lawson_person_first_name,
          m2.lawson_person_middle_name,
          m2.lawson_person_department_code,
          m2.lawson_person_job_class_code,
          m2.source_system_code,
          m2.dw_last_update_date_time
        FROM
          (
            SELECT
                m1.im_domain_id,
                m1.lawson_person_user_id,
                m1.lawson_empl_status_code,
                m1.lawson_person_supv_user_id,
                m1.lawson_person_empl_num,
                m1.lawson_person_action_name,
                m1.lawson_person_hire_date,
                m1.lawson_person_posn_start_date,
                m1.lawson_person_termn_date,
                m1.lawson_person_last_name,
                m1.lawson_person_first_name,
                m1.lawson_person_middle_name,
                m1.lawson_person_department_code,
                m1.lawson_person_job_class_code,
                m1.source_system_code,
                m1.dw_last_update_date_time,
                row_number() OVER (PARTITION BY upper(m1.lawson_person_user_id) ORDER BY m1.status_rank, m1.lawson_person_termn_date DESC, m1.lawson_person_hire_date DESC, upper(m1.prcs_lvl) DESC) AS row_rank
              FROM
                (
                  SELECT
                      '21' AS im_domain_id,
                      t2.cmpy_num AS lawson_person_user_id,
                      t1.empl_stts AS lawson_empl_status_code,
                      t3.cmpy_num AS lawson_person_supv_user_id,
                      t1.empl AS lawson_person_empl_num,
                      t4.actn_cd AS lawson_person_action_name,
                      t1.hire_dt AS lawson_person_hire_date,
                      t1.hire_dt AS lawson_person_posn_start_date,
                      t1.termn_dt AS lawson_person_termn_date,
                      t1.lst_nm AS lawson_person_last_name,
                      t1.frst_nm AS lawson_person_first_name,
                      t1.midl_nm AS lawson_person_middle_name,
                      t5.dept AS lawson_person_department_code,
                      t5.job_cl AS lawson_person_job_class_code,
                      t1.prcs_lvl,
                      'L' AS source_system_code,
                      datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                      CASE
                        WHEN rtrim(t1.empl_stts) = '01'
                         AND coalesce(t1.termn_dt, DATE '2000-01-01') = DATE '2000-01-01' THEN 1
                        WHEN rtrim(t1.empl_stts) = '02'
                         AND coalesce(t1.termn_dt, DATE '2000-01-01') = DATE '2000-01-01' THEN 2
                        WHEN rtrim(t1.empl_stts) = '03'
                         AND coalesce(t1.termn_dt, DATE '2000-01-01') = DATE '2000-01-01' THEN 3
                        WHEN rtrim(t1.empl_stts) = '04'
                         AND coalesce(t1.termn_dt, DATE '2000-01-01') = DATE '2000-01-01' THEN 4
                        WHEN rtrim(t1.empl_stts) = '05'
                         AND coalesce(t1.termn_dt, DATE '2000-01-01') = DATE '2000-01-01' THEN 5
                        WHEN rtrim(t1.empl_stts) = '06'
                         AND coalesce(t1.termn_dt, DATE '2000-01-01') = DATE '2000-01-01' THEN 6
                        WHEN rtrim(t1.empl_stts) = '99'
                         AND coalesce(t1.termn_dt, DATE '2000-01-01') = DATE '2000-01-01' THEN 7
                        WHEN rtrim(t1.empl_stts) = '08'
                         AND coalesce(t1.termn_dt, DATE '2000-01-01') <> DATE '2000-01-01' THEN 8
                        WHEN rtrim(t1.empl_stts) = '07'
                         AND coalesce(t1.termn_dt, DATE '2000-01-01') <> DATE '2000-01-01' THEN 9
                        WHEN rtrim(t1.empl_stts) = '99'
                         AND coalesce(t1.termn_dt, DATE '2000-01-01') <> DATE '2000-01-01' THEN 10
                        ELSE 11
                      END AS status_rank
                    FROM
                      `hca-hin-dev-cur-comp`.edwim_staging.lawsn_esaf_employee AS t1
                      INNER JOIN `hca-hin-dev-cur-comp`.edwim_staging.lawsn_esaf_personnel_employee AS t2 ON t2.empl = t1.empl
                       AND t2.cmpy = t1.cmpy
                      LEFT OUTER JOIN (
                        SELECT
                            s1.cmpy,
                            s1.supv_cd,
                            s2.cmpy_num
                          FROM
                            `hca-hin-dev-cur-comp`.edwim_staging.lawsn_esaf_supervisor AS s1
                            INNER JOIN `hca-hin-dev-cur-comp`.edwim_staging.lawsn_esaf_personnel_employee AS s2 ON s2.empl = s1.empl
                      ) AS t3 ON t1.cmpy = t3.cmpy
                       AND upper(rtrim(t1.supv)) = upper(rtrim(t3.supv_cd))
                      LEFT OUTER JOIN (
                        SELECT
                            s3.actn_cd,
                            s3.cmpy,
                            s3.empl
                          FROM
                            (
                              SELECT
                                  is3.actn_cd,
                                  is3.cmpy,
                                  is3.empl,
                                  row_number() OVER (PARTITION BY is3.empl ORDER BY is3.eff_dt DESC) AS row_rank
                                FROM
                                  `hca-hin-dev-cur-comp`.edwim_staging.lawsn_esaf_personnel_action AS is3
                            ) AS s3
                          WHERE s3.row_rank = 1
                      ) AS t4 ON t1.cmpy = t4.cmpy
                       AND t1.empl = t4.empl
                      LEFT OUTER JOIN (
                        SELECT
                            s4.cmpy,
                            s4.posn,
                            s4.dept,
                            s4.job_cd,
                            s4.job_cl
                          FROM
                            (
                              SELECT DISTINCT
                                  is4.cmpy,
                                  is4.posn,
                                  is4.dept,
                                  is4.job_cd,
                                  is5.job_cl,
                                  row_number() OVER (PARTITION BY is4.cmpy, upper(is4.posn) ORDER BY is4.eff_dt DESC) AS row_rank
                                FROM
                                  `hca-hin-dev-cur-comp`.edwim_staging.lawsn_esaf_personnel_position AS is4
                                  INNER JOIN `hca-hin-dev-cur-comp`.edwim_staging.lawsn_esaf_job AS is5 ON is4.cmpy = is5.cmpy
                                   AND upper(rtrim(is4.job_cd)) = upper(rtrim(is5.job_cd))
                            ) AS s4
                          WHERE s4.row_rank = 1
                      ) AS t5 ON t1.cmpy = t5.cmpy
                       AND upper(rtrim(t1.posn)) = upper(rtrim(t5.posn))
                ) AS m1
          ) AS m2
        WHERE m2.row_rank = 1
    ) AS a
;
