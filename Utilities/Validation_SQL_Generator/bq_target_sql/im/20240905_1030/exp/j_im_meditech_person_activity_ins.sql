-- Translation time: 2024-09-05T15:31:41.992Z
-- Translation job ID: 2a0106aa-7f86-44a1-bb13-2e1417e5dce4
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240905_1030/input/exp/j_im_meditech_person_activity_ins.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', coalesce(count(*), 0)) AS source_string
  FROM
    (
      SELECT
          t1.im_domain_id,
          t1.mt_user_id,
          t1.esaf_activity_date,
          t1.access_rule_id,
          im_person_inactivate_sw AS im_person_inactivate_sw,
          'M' AS source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
        FROM
          (
            SELECT
                s1.im_domain_id,
                s1.mt_user_id,
                s1.esaf_activity_date,
                CASE
                  WHEN s1.mt_alias_exempt_sw = 1 THEN 0
                  WHEN s1.mt_excluded_user_sw = 1 THEN 0
                  WHEN s1.mt_staff_pm_user_sw = 1 THEN 0
                  WHEN s4.lawson_person_new_hire_sw = 1 THEN 0
                  WHEN s4.lawson_excluded_job_class_sw = 1 THEN 0
                  WHEN s4.lawson_excluded_department_sw = 1 THEN 0
                  WHEN s2.ad_user_new_sw = 1 THEN 0
                  WHEN s2.ad_group_exempt_sw = 1 THEN 0
                  WHEN s3.ad_employee_type_id = 41 THEN 0
                  WHEN s3.ad_employee_type_id = 16 THEN 0
                  WHEN s1.mt_user_activity_sw = 0 THEN 4
                  WHEN s2.ad_user_active_sw <> 1 THEN 5
                  WHEN s3.ad_account_user_id IS NULL THEN 6
                  ELSE 0
                END AS access_rule_id
              FROM
                -- BEGIN EXCLUSIONS
                -- EXEMPT ALIASES EXCLUSION RULE #4 & #5
                -- EXEPMT MEDITECH CORPORATE USERS EXCLUSION RULE #6
                -- STAFF PM USER EXCLUSION RULE #8
                -- NEW LAWSON EMPLOYEE HIRE EXCLUSION RULE #11
                -- EXEMPT LAWSON JOB CLASS EXCLUSION RULE #12
                -- EXEMPT LAWSON DEPARTMENTS EXCLUSION RULE #13
                -- NEW AD NON EMPLOYEE HIRING EXCLUSION RULE #15
                -- EXEMPT AD GROUPS EXCLUSION RULE #16
                -- EXEMPT APP AD USER TYPE
                -- EXEMPT LIP AD USER TYPE
                -- BEGIN DISABLEMENTS
                -- MIS ACCOUNT WITH SYSTEM LOGON >1 YEAR DISABLMENT RULE #1
                -- CANNOT BE MATCHED TO AN ENABLED AD ACCOUNT DISABLEMENT RULE #2
                -- UNLINKED AD USER ACCOUNT DISABLEMENT RULE #3
                `hca-hin-dev-cur-comp`.edwim_base_views.meditech_user_activity AS s1
                LEFT OUTER JOIN `hca-hin-dev-cur-comp`.edwim_base_views.ad_user_activity AS s2 ON upper(rtrim(s1.mt_user_id)) = upper(rtrim(s2.ad_account_user_id))
                 AND s1.esaf_activity_date = s2.esaf_activity_date
                LEFT OUTER JOIN `hca-hin-dev-cur-comp`.edwim_base_views.ad_account AS s3 ON upper(rtrim(s1.mt_user_id)) = upper(rtrim(s3.ad_account_user_id))
                LEFT OUTER JOIN `hca-hin-dev-cur-comp`.edwim_base_views.lawson_user_activity AS s4 ON upper(rtrim(s1.mt_user_id)) = upper(rtrim(s4.lawson_person_user_id))
                 AND s1.esaf_activity_date = s4.esaf_activity_date
          ) AS t1
          CROSS JOIN UNNEST(ARRAY[
            CASE
              WHEN t1.access_rule_id = 0 THEN t1.access_rule_id
              ELSE 1
            END
          ]) AS im_person_inactivate_sw
        WHERE im_person_inactivate_sw = 1
    ) AS a
;
