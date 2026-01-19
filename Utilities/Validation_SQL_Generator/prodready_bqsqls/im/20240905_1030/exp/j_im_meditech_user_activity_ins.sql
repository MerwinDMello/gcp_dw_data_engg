-- Translation time: 2024-09-05T15:31:41.992Z
-- Translation job ID: 2a0106aa-7f86-44a1-bb13-2e1417e5dce4
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240905_1030/input/exp/j_im_meditech_user_activity_ins.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', coalesce(count(*), 0)) AS source_string
FROM
  (SELECT t1.im_domain_id,
          t1.mt_user_id,
          current_date('US/Central') AS esaf_activity_date,
          t1.mt_user_last_activity_date,
          t1.mt_user_activity_sw,
          t1.mt_excluded_user_sw,
          t1.mt_staff_pm_user_sw,
          t1.mt_alias_exempt_sw,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT DISTINCT s1.im_domain_id,
                      s1.mt_user_id,
                      other_platform_activity_date AS other_platform_activity_date,
                      CASE
                          WHEN s1.mt_user_last_activity_date IS NULL THEN other_platform_activity_date
                          WHEN other_platform_activity_date IS NULL THEN s1.mt_user_last_activity_date
                          WHEN s1.mt_user_last_activity_date >= other_platform_activity_date THEN s1.mt_user_last_activity_date
                          ELSE other_platform_activity_date
                      END AS mt_user_last_activity_date,
                      CASE
                          WHEN s1.mt_user_last_activity_date IS NULL THEN 0
                          WHEN date_diff(current_date('US/Central'), s1.mt_user_last_activity_date, DAY) > 365 THEN 0
                          ELSE 1
                      END AS mt_user_activity_sw, -- SYSTEM LOGON > 1 YEAR DISABLEMENT RULE #1
 CASE
     WHEN rtrim(s1.mt_user_mnemonic_cs) IN('1PDSRB0154',
                                           '1PDJRH1716',
                                           '1PDMMW6006') THEN 1
     ELSE 0
 END AS mt_excluded_user_sw, -- CORPORATE EXEMPT USERS EXCLUSION RULE #6
 CASE
     WHEN s2.ntlogin IS NOT NULL THEN 1
     ELSE 0
 END AS mt_staff_pm_user_sw, -- STAFF PM USERS EXCLUSION RULE #8
 CASE
     WHEN s3.prctnr_mnem_cs IS NOT NULL THEN 1
     ELSE 0
 END AS mt_alias_exempt_sw
      FROM -- MT EXEMPT ALIASES EXCLUSION RULE #4 & #5
 {{ params.param_im_base_views_dataset_name }}.meditech_user AS s1
      LEFT OUTER JOIN {{ params.param_im_stage_dataset_name }}.staff_pm_users AS s2 ON upper(rtrim(s1.mt_user_id)) = upper(rtrim(s2.ntlogin))
      LEFT OUTER JOIN
        (SELECT t1_0.im_domain_id,
                t2.ntwk_mnem_cs,
                t2.prctnr_mnem_cs,
                t2.prctnr_alias_nm
         FROM {{ params.param_im_base_views_dataset_name }}.ref_im_domain AS t1_0
         INNER JOIN
           (SELECT DISTINCT prctnr_actvt_dtl.ntwk_mnem_cs,
                            trim(prctnr_actvt_dtl.prctnr_mnem_cs) AS prctnr_mnem_cs,
                            prctnr_actvt_dtl.prctnr_alias_nm,
                            mt_user_exmpt AS mt_user_exmpt
            FROM {{ params.param_im_base_views_dataset_name }}.prctnr_actvt_dtl
            CROSS JOIN UNNEST(ARRAY[ CASE upper(rtrim(prctnr_actvt_dtl.prctnr_alias_nm))
                                         WHEN 'INTERFACE' THEN 1
                                         WHEN 'NON-HCA FACILITY' THEN 1
                                         WHEN 'OPO ORGAN PROCUREMENT' THEN 1
                                         WHEN 'OTHER' THEN 1
                                         WHEN 'OTHER – CPOE' THEN 1
                                         WHEN 'OTHER – DISASTER' THEN 1
                                         WHEN 'OTHER - DISASTER ID' THEN 1
                                         WHEN 'OTHER – DOWNTIME' THEN 1
                                         WHEN 'OTHER – ROBOT' THEN 1
                                         WHEN 'PAGER' THEN 1
                                         WHEN 'PAGER – PERSON' THEN 1
                                         WHEN 'PHONE' THEN 1
                                         WHEN 'SCRIPT' THEN 1
                                         WHEN 'TEMPLATE' THEN 1
                                         WHEN 'TEMPLATE - PARALLON SC' THEN 1
                                         WHEN 'TEMPLATE – PBPG' THEN 1
                                         WHEN 'TEMPLATE – PWS' THEN 1
                                         WHEN 'TRACKER' THEN 1
                                         WHEN 'USER' THEN 1
                                         WHEN 'USER – CORPORATE' THEN 1
                                         WHEN 'USER – EVS' THEN 1
                                         WHEN 'USER – FAN' THEN 1
                                         WHEN 'USER – PARALLON' THEN 1
                                         WHEN 'USER - PARALLON CODING' THEN 1
                                         WHEN 'USER - PARALLON HEALTHPORT' THEN 1
                                         WHEN 'USER - PARALLON TECHNOLOGY SOLUTIONS' THEN 1
                                         WHEN 'USER - PARALLON WORKFORCE SOLUTIONS' THEN 1
                                         WHEN 'USER – PWS' THEN 1
                                         WHEN 'USER - PWS LSC' THEN 1
                                         WHEN 'USER – SCRI, VENDOR' THEN 1
                                         ELSE 0
                                     END ]) AS mt_user_exmpt
            WHERE mt_user_exmpt = 1 ) AS t2 ON rtrim(t2.ntwk_mnem_cs) = rtrim(t1_0.im_domain_name)) AS s3 ON s1.im_domain_id = s3.im_domain_id
      AND rtrim(s1.mt_user_mnemonic_cs) = rtrim(s3.prctnr_mnem_cs)
      LEFT OUTER JOIN {{ params.param_im_base_views_dataset_name }}.pk_user AS s4 ON s1.im_domain_id = s4.im_domain_id
      AND upper(rtrim(s1.mt_user_id)) = upper(rtrim(s4.pk_user_id))
      LEFT OUTER JOIN
        (SELECT DISTINCT h2.mt_domain_id,
                         h1.hpf_user_id,
                         h1.hpf_user_last_activity_date
         FROM {{ params.param_im_base_views_dataset_name }}.hpf_account AS h1
         INNER JOIN {{ params.param_im_base_views_dataset_name }}.platform_domain_xwalk AS h2 ON h1.im_domain_id = h2.mt_domain_id) AS s5 ON upper(rtrim(s1.mt_user_id)) = upper(rtrim(s5.hpf_user_id))
      AND s1.im_domain_id = s5.mt_domain_id
      CROSS JOIN UNNEST(ARRAY[ CASE
                                   WHEN s5.hpf_user_last_activity_date IS NULL THEN s4.pk_user_last_activity_date
                                   WHEN s4.pk_user_last_activity_date IS NULL THEN s5.hpf_user_last_activity_date
                                   WHEN s5.hpf_user_last_activity_date >= s4.pk_user_last_activity_date THEN s5.hpf_user_last_activity_date
                                   ELSE s4.pk_user_last_activity_date
                               END ]) AS other_platform_activity_date) AS t1) AS a