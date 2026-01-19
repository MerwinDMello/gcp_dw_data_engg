BEGIN
  DECLARE dup_count int64;
  declare  
    current_ts datetime;
  set 
    current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);

  BEGIN TRANSACTION;

  DELETE FROM {{ params.param_hr_core_dataset_name  }}.bi_psat_question_level_score_smry WHERE 1 = 1;

  INSERT INTO {{ params.param_hr_core_dataset_name  }}.bi_psat_question_level_score_smry (qtr_id, qtr_desc_dss, corporate_code, corporate_type_code, corporate_name, group_code, group_type_code, group_name, division_code, division_type_code, division_name, market_code, market_type_code, market_name, company_code, coid, coid_type_code, coid_name, vendor_assigned_unit_text, org_level_code, survey_category_code, domain_id, domain_desc, measure_id_text, question_id, question_short_name, question_desc, aggregated_top_box_score_num, survey_response_cnt, respondent_cnt, top_box_pct, source_system_code, dw_last_update_date_time)
    SELECT
        -- Unit_Code,
        -- --Nursing Unit Level
        dt.qtr_id,
        dt.qtr_desc_dss,
        ff2.corporate_code,
        ff2.corporate_type_code,
        ff2.corporate_name,
        ff2.group_code,
        ff2.group_type_code,
        ff2.group_name,
        ff2.division_code,
        ff2.division_type_code,
        ff2.division_name,
        ff2.market_code,
        ff2.market_type_code,
        ff2.market_name,
        ff2.company_code,
        ff.coid,
        ff.coid_type_code,
        ff.coid_name,
        max(coalesce(sr.vendor_assigned_unit_text, 'Unassigned')) AS vendor_assigned_unit_text,
        'L' AS org_level_code,
        rs.survey_category_code,
        psd.domain_id,
        psd.domain_desc,
        sq.measure_id_text,
        sq.question_id,
        sq.question_short_name,
        sq.question_desc,
        sum(CASE
          WHEN qx.domain_id IN(
            75681, 82303, 50010
          )
           AND sr.response_value_text = '1' THEN 1
          ELSE sr.top_box_score_num
        END) AS aggregated_top_box_score_num,
        count(rpd.respondent_id) AS survey_response_cnt,
        count(DISTINCT rpd.respondent_id) AS respondent_cnt,
        sum(CASE
          WHEN qx.domain_id IN(
            75681, 82303, 50010
          )
           AND sr.response_value_text = '1' THEN 1
          ELSE sr.top_box_score_num
        END) / CAST(count(rpd.respondent_id) as NUMERIC) * 100 - CASE
          WHEN mode_adjustment_amt IS NOT NULL THEN mode_adjustment_amt
          ELSE 0
        END AS top_box_pct,
        'H' AS source_system_code,
        current_ts AS dw_last_update_date_time
      FROM
        {{ params.param_hr_base_views_dataset_name }}.survey_response AS sr
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.survey_question AS sq ON sr.survey_question_sid = sq.survey_question_sid
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.respondent_patient_detail AS rpd ON rpd.respondent_id = sr.respondent_id
         AND rpd.survey_receive_date = sr.survey_receive_date
         AND rpd.survey_sid = sq.survey_sid
        INNER JOIN {{ params.param_pub_views_dataset_name }}.lu_date AS dt ON rpd.discharge_date = dt.date_id
        INNER JOIN (
          SELECT
              lu_date.qtr_desc,
              lu_date.qtr_id,
              max(lu_date.date_id) AS eoq_date,
              max(lu_date.date_id) + 42 AS in_survey_qtr_end_date,
              max(lu_date.date_id) + 63 AS oas_survey_qtr_end_date
            FROM
              {{ params.param_pub_views_dataset_name }}.lu_date
            WHERE lu_date.year_id <= 3000
            GROUP BY 1, 2
        ) AS qcp ON qcp.qtr_id = dt.qtr_id
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.patient_satisfaction_alt_org_hierarchy AS alt ON alt.coid = rpd.coid
         AND alt.company_code = rpd.company_code
        INNER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS ff ON alt.coid = ff.coid
         AND alt.company_code = ff.company_code
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.fixed_respondent_patient_detail AS frpd ON frpd.respondent_id = sr.respondent_id
         AND frpd.survey_sid = sq.survey_sid
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_phone_mode_adjustment AS pm ON sq.measure_id_text = pm.measure_id_text
         AND sr.time_name_child BETWEEN pm.eff_from_date AND pm.eff_to_date
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.question_legacy_survey_xwalk AS qx ON qx.question_id = sq.question_id
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_patient_satisfaction_domain AS psd ON psd.domain_id = qx.domain_id
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_survey AS rs ON rs.survey_sid = sq.survey_sid
        INNER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS ff2 ON alt.parent_coid = ff2.coid
      WHERE /*** Include only domain_ids where domain_id is not null or zero ***/ psd.domain_id > 0
       AND /*** For IN, use CMS_Submit_Ind = 'Y' if closed quarter. Use CMS_Submit_Preliminary_Ind if open quarter. For AS, use CMS_Submit_Preliminary_Ind. Else return***/ /*** Only include ER surveys that were received within 42 days after the end of the quarter ***/ (upper(rs.survey_category_code) = 'IN'
       AND upper(sr.cms_submit_ind) = 'Y'
       AND qcp.in_survey_qtr_end_date < DATE(current_ts)
       OR upper(rs.survey_category_code) = 'IN'
       AND upper(sr.cms_submit_preliminary_ind) = 'Y'
       AND qcp.in_survey_qtr_end_date >= DATE(current_ts)
       OR rs.survey_category_code NOT IN(
        'IN', 'ER'
      )
       OR upper(rs.survey_category_code) = 'ER'
       AND sr.survey_receive_date <= qcp.in_survey_qtr_end_date)
       AND /*** Limitor to Adjusted Sample Indicator (from Press Ganey)  ***/ upper(sr.adjusted_sample_ind) = 'Y'
       AND /*** Limitor to past 4 years  ***/ sr.time_name_child > date_add(DATE(current_ts), interval -48 MONTH)
       AND upper(sq.source_system_code) = 'H'
       AND upper(trim(rs.source_system_code)) = 'H'
       AND upper(sr.source_system_code) = 'H'
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, upper(coalesce(sr.vendor_assigned_unit_text, 'Unassigned')), 20, 21, 22, 23, 24, 25, 26, 27, /*** Group by using Ints and need to add Mode_Adjustment_Amt if applicable ***/ mode_adjustment_amt
      HAVING /***** Response Limitor  - Need at least 7 response in the Denominator (can be lowered with business owner approval to 5) *****/ count(DISTINCT rpd.respondent_id) > 6
    UNION ALL
    SELECT
        -- --COID LEVEL
        dt.qtr_id,
        dt.qtr_desc_dss,
        ff2.corporate_code,
        ff2.corporate_type_code,
        ff2.corporate_name,
        ff2.group_code,
        ff2.group_type_code,
        ff2.group_name,
        ff2.division_code,
        ff2.division_type_code,
        ff2.division_name,
        ff2.market_code,
        ff2.market_type_code,
        ff2.market_name,
        ff2.company_code,
        ff.coid,
        ff.coid_type_code,
        ff.coid_name,
        '99999' AS vendor_assigned_unit_text,
        max(ff.coid_type_code) AS org_level_code,
        rs.survey_category_code,
        psd.domain_id,
        psd.domain_desc,
        sq.measure_id_text,
        sq.question_id,
        sq.question_short_name,
        sq.question_desc,
        sum(CASE
          WHEN qx.domain_id IN(
            75681, 82303, 50010
          )
           AND sr.response_value_text = '1' THEN 1
          ELSE sr.top_box_score_num
        END) AS aggregated_top_box_score_num,
        count(rpd.respondent_id) AS survey_response_cnt,
        count(DISTINCT rpd.respondent_id) AS respondent_cnt,
        sum(CASE
          WHEN qx.domain_id IN(
            75681, 82303, 50010
          )
           AND sr.response_value_text = '1' THEN 1
          ELSE sr.top_box_score_num
        END) / CAST(count(rpd.respondent_id) as NUMERIC) * 100 - CASE
          WHEN mode_adjustment_amt IS NOT NULL THEN mode_adjustment_amt
          ELSE 0
        END AS top_box_pct,
        'H' AS source_system_code,
        current_ts AS dw_last_update_date_time
      FROM
        {{ params.param_hr_base_views_dataset_name }}.survey_response AS sr
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.survey_question AS sq ON sr.survey_question_sid = sq.survey_question_sid
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.respondent_patient_detail AS rpd ON rpd.respondent_id = sr.respondent_id
         AND rpd.survey_receive_date = sr.survey_receive_date
         AND rpd.survey_sid = sq.survey_sid
        INNER JOIN {{ params.param_pub_views_dataset_name }}.lu_date AS dt ON rpd.discharge_date = dt.date_id
        INNER JOIN (
          SELECT
              lu_date.qtr_desc,
              lu_date.qtr_id,
              max(lu_date.date_id) AS eoq_date,
              max(lu_date.date_id) + 42 AS in_survey_qtr_end_date,
              max(lu_date.date_id) + 63 AS oas_survey_qtr_end_date
            FROM
              {{ params.param_pub_views_dataset_name }}.lu_date
            WHERE lu_date.year_id <= 3000
            GROUP BY 1, 2
        ) AS qcp ON qcp.qtr_id = dt.qtr_id
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.patient_satisfaction_alt_org_hierarchy AS alt ON alt.coid = rpd.coid
         AND alt.company_code = rpd.company_code
        INNER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS ff ON alt.coid = ff.coid
         AND alt.company_code = ff.company_code
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.fixed_respondent_patient_detail AS frpd ON frpd.respondent_id = sr.respondent_id
         AND frpd.survey_sid = sq.survey_sid
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_phone_mode_adjustment AS pm ON sq.measure_id_text = pm.measure_id_text
         AND sr.time_name_child BETWEEN pm.eff_from_date AND pm.eff_to_date
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.question_legacy_survey_xwalk AS qx ON qx.question_id = sq.question_id
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_patient_satisfaction_domain AS psd ON psd.domain_id = qx.domain_id
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_survey AS rs ON rs.survey_sid = sq.survey_sid
        INNER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS ff2 ON alt.parent_coid = ff2.coid
      WHERE /*** Include only domain_ids where domain_id is not null or zero ***/ psd.domain_id > 0
       AND /*** For IN, use CMS_Submit_Ind = 'Y' if closed quarter. Use CMS_Submit_Preliminary_Ind if open quarter. For AS, use CMS_Submit_Preliminary_Ind. Else return***/ /*** Only include ER surveys that were received within 42 days after the end of the quarter ***/ (upper(rs.survey_category_code) = 'IN'
       AND upper(sr.cms_submit_ind) = 'Y'
       AND qcp.in_survey_qtr_end_date <  DATE(current_ts)
       OR upper(rs.survey_category_code) = 'IN'
       AND upper(sr.cms_submit_preliminary_ind) = 'Y'
       AND qcp.in_survey_qtr_end_date >=  DATE(current_ts)
       OR rs.survey_category_code NOT IN(
        'IN', 'ER'
      )
       OR upper(rs.survey_category_code) = 'ER'
       AND sr.survey_receive_date <= qcp.in_survey_qtr_end_date)
       AND /*** Limitor to Adjusted Sample Indicator (from Press Ganey)  ***/ upper(sr.adjusted_sample_ind) = 'Y'
       AND /*** Limitor to past 4 years  ***/ sr.time_name_child > date_add(DATE(current_ts), interval -48 MONTH)
       AND upper(sq.source_system_code) = 'H'
       AND upper(trim(rs.source_system_code)) = 'H'
       AND upper(sr.source_system_code) = 'H'
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,upper(ff.coid_type_code), 21, 22, 23, 24, 25, 26, 27, /*** Group by using Ints and need to add Mode_Adjustment_Amt if applicable ***/ mode_adjustment_amt
      HAVING /***** Response Limitor  - Need at least 7 response in the Denominator (can be lowered with business owner approval to 5) *****/ count(DISTINCT rpd.respondent_id) > 6
    UNION ALL
    SELECT
        -- -Market
        dt.qtr_id,
        dt.qtr_desc_dss,
        ff2.corporate_code,
        ff2.corporate_type_code,
        ff2.corporate_name,
        ff2.group_code,
        ff2.group_type_code,
        ff2.group_name,
        ff2.division_code,
        ff2.division_type_code,
        ff2.division_name,
        ff2.market_code,
        ff2.market_type_code,
        ff2.market_name,
        ff2.company_code,
        '99999' AS coid,
        CAST(NULL as STRING) AS coid_type_code,
        CAST(NULL as STRING) AS coid_name,
        '99999' AS vendor_assigned_unit_text,
        max(ff2.market_type_code) AS org_level_code,
        rs.survey_category_code,
        psd.domain_id,
        psd.domain_desc,
        sq.measure_id_text,
        sq.question_id,
        sq.question_short_name,
        sq.question_desc,
        sum(CASE
          WHEN qx.domain_id IN(
            75681, 82303, 50010
          )
           AND sr.response_value_text = '1' THEN 1
          ELSE sr.top_box_score_num
        END) AS aggregated_top_box_score_num,
        count(rpd.respondent_id) AS survey_response_cnt,
        count(DISTINCT rpd.respondent_id) AS respondent_cnt,
        sum(CASE
          WHEN qx.domain_id IN(
            75681, 82303, 50010
          )
           AND sr.response_value_text = '1' THEN 1
          ELSE sr.top_box_score_num
        END) / CAST(count(rpd.respondent_id) as NUMERIC) * 100 - CASE
          WHEN mode_adjustment_amt IS NOT NULL THEN mode_adjustment_amt
          ELSE 0
        END AS top_box_pct,
        'H' AS source_system_code,
        current_ts AS dw_last_update_date_time
      FROM
        {{ params.param_hr_base_views_dataset_name }}.survey_response AS sr
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.survey_question AS sq ON sr.survey_question_sid = sq.survey_question_sid
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.respondent_patient_detail AS rpd ON rpd.respondent_id = sr.respondent_id
         AND rpd.survey_receive_date = sr.survey_receive_date
         AND rpd.survey_sid = sq.survey_sid
        INNER JOIN {{ params.param_pub_views_dataset_name }}.lu_date AS dt ON rpd.discharge_date = dt.date_id
        INNER JOIN (
          SELECT
              lu_date.qtr_desc,
              lu_date.qtr_id,
              max(lu_date.date_id) AS eoq_date,
              max(lu_date.date_id) + 42 AS in_survey_qtr_end_date,
              max(lu_date.date_id) + 63 AS oas_survey_qtr_end_date
            FROM
              {{ params.param_pub_views_dataset_name }}.lu_date
            WHERE lu_date.year_id <= 3000
            GROUP BY 1, 2
        ) AS qcp ON qcp.qtr_id = dt.qtr_id
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.patient_satisfaction_alt_org_hierarchy AS alt ON alt.coid = rpd.coid
         AND alt.company_code = rpd.company_code
        INNER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS ff ON alt.coid = ff.coid
         AND alt.company_code = ff.company_code
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.fixed_respondent_patient_detail AS frpd ON frpd.respondent_id = sr.respondent_id
         AND frpd.survey_sid = sq.survey_sid
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_phone_mode_adjustment AS pm ON sq.measure_id_text = pm.measure_id_text
         AND sr.time_name_child BETWEEN pm.eff_from_date AND pm.eff_to_date
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.question_legacy_survey_xwalk AS qx ON qx.question_id = sq.question_id
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_patient_satisfaction_domain AS psd ON psd.domain_id = qx.domain_id
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_survey AS rs ON rs.survey_sid = sq.survey_sid
        INNER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS ff2 ON alt.parent_coid = ff2.coid
      WHERE /*** Include only domain_ids where domain_id is not null or zero ***/ psd.domain_id > 0
       AND /*** For IN, use CMS_Submit_Ind = 'Y' if closed quarter. Use CMS_Submit_Preliminary_Ind if open quarter. For AS, use CMS_Submit_Preliminary_Ind. Else return***/ /*** Only include ER surveys that were received within 42 days after the end of the quarter ***/ (upper(rs.survey_category_code) = 'IN'
       AND upper(sr.cms_submit_ind) = 'Y'
       AND qcp.in_survey_qtr_end_date <  DATE(current_ts)
       OR upper(rs.survey_category_code) = 'IN'
       AND upper(sr.cms_submit_preliminary_ind) = 'Y'
       AND qcp.in_survey_qtr_end_date >=  DATE(current_ts)
       OR rs.survey_category_code NOT IN(
        'IN', 'ER'
      )
       OR upper(rs.survey_category_code) = 'ER'
       AND sr.survey_receive_date <= qcp.in_survey_qtr_end_date)
       AND /*** Limitor to Adjusted Sample Indicator (from Press Ganey)  ***/ upper(sr.adjusted_sample_ind) = 'Y'
       AND /*** Limitor to past 4 years  ***/ sr.time_name_child > date_add(DATE(current_ts), interval -48 MONTH)
       AND upper(sq.source_system_code) = 'H'
       AND upper(trim(rs.source_system_code)) = 'H'
       AND upper(sr.source_system_code) = 'H'
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,19, upper(ff2.market_type_code), 21, 22, 23, 24, 25, 26, 27, /*** Group by using Ints and need to add Mode_Adjustment_Amt if applicable ***/ mode_adjustment_amt
      HAVING /***** Response Limitor  - Need at least 7 response in the Denominator (can be lowered with business owner approval to 5) *****/ count(DISTINCT rpd.respondent_id) > 6
    UNION ALL
    SELECT
        -- -DIVISION
        dt.qtr_id,
        dt.qtr_desc_dss,
        ff2.corporate_code,
        ff2.corporate_type_code,
        ff2.corporate_name,
        ff2.group_code,
        ff2.group_type_code,
        ff2.group_name,
        ff2.division_code,
        ff2.division_type_code,
        ff2.division_name,
        '99999' AS market_code,
        CAST(NULL as STRING) AS market_type_code,
        CAST(NULL as STRING) AS market_name,
        ff2.company_code,
        '99999' AS coid,
        CAST(NULL as STRING) AS coid_type_code,
        CAST(NULL as STRING) AS coid_name,
        '99999' AS vendor_assigned_unit_text,
        max(ff2.division_type_code) AS org_level_code,
        rs.survey_category_code,
        psd.domain_id,
        psd.domain_desc,
        sq.measure_id_text,
        sq.question_id,
        sq.question_short_name,
        sq.question_desc,
        sum(CASE
          WHEN qx.domain_id IN(
            75681, 82303, 50010
          )
           AND sr.response_value_text = '1' THEN 1
          ELSE sr.top_box_score_num
        END) AS aggregated_top_box_score_num,
        count(rpd.respondent_id) AS survey_response_cnt,
        count(DISTINCT rpd.respondent_id) AS respondent_cnt,
        sum(CASE
          WHEN qx.domain_id IN(
            75681, 82303, 50010
          )
           AND sr.response_value_text = '1' THEN 1
          ELSE sr.top_box_score_num
        END) / CAST(count(rpd.respondent_id) as NUMERIC) * 100 - CASE
          WHEN mode_adjustment_amt IS NOT NULL THEN mode_adjustment_amt
          ELSE 0
        END AS top_box_pct,
        'H' AS source_system_code,
       current_ts AS dw_last_update_date_time
      FROM
        {{ params.param_hr_base_views_dataset_name }}.survey_response AS sr
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.survey_question AS sq ON sr.survey_question_sid = sq.survey_question_sid
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.respondent_patient_detail AS rpd ON rpd.respondent_id = sr.respondent_id
         AND rpd.survey_receive_date = sr.survey_receive_date
         AND rpd.survey_sid = sq.survey_sid
        INNER JOIN {{ params.param_pub_views_dataset_name }}.lu_date AS dt ON rpd.discharge_date = dt.date_id
        INNER JOIN (
          SELECT
              lu_date.qtr_desc,
              lu_date.qtr_id,
              max(lu_date.date_id) AS eoq_date,
              max(lu_date.date_id) + 42 AS in_survey_qtr_end_date,
              max(lu_date.date_id) + 63 AS oas_survey_qtr_end_date
            FROM
              {{ params.param_pub_views_dataset_name }}.lu_date
            WHERE lu_date.year_id <= 3000
            GROUP BY 1, 2
        ) AS qcp ON qcp.qtr_id = dt.qtr_id
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.patient_satisfaction_alt_org_hierarchy AS alt ON alt.coid = rpd.coid
         AND alt.company_code = rpd.company_code
        INNER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS ff ON alt.coid = ff.coid
         AND alt.company_code = ff.company_code
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.fixed_respondent_patient_detail AS frpd ON frpd.respondent_id = sr.respondent_id
         AND frpd.survey_sid = sq.survey_sid
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_phone_mode_adjustment AS pm ON sq.measure_id_text = pm.measure_id_text
         AND sr.time_name_child BETWEEN pm.eff_from_date AND pm.eff_to_date
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.question_legacy_survey_xwalk AS qx ON qx.question_id = sq.question_id
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_patient_satisfaction_domain AS psd ON psd.domain_id = qx.domain_id
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_survey AS rs ON rs.survey_sid = sq.survey_sid
        INNER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS ff2 ON alt.parent_coid = ff2.coid
      WHERE /*** Include only domain_ids where domain_id is not null or zero ***/ psd.domain_id > 0
       AND /*** For IN, use CMS_Submit_Ind = 'Y' if closed quarter. Use CMS_Submit_Preliminary_Ind if open quarter. For AS, use CMS_Submit_Preliminary_Ind. Else return***/ /*** Only include ER surveys that were received within 42 days after the end of the quarter ***/ (upper(rs.survey_category_code) = 'IN'
       AND upper(sr.cms_submit_ind) = 'Y'
       AND qcp.in_survey_qtr_end_date <  DATE(current_ts)
       OR upper(rs.survey_category_code) = 'IN'
       AND upper(sr.cms_submit_preliminary_ind) = 'Y'
       AND qcp.in_survey_qtr_end_date >=  DATE(current_ts)
       OR rs.survey_category_code NOT IN(
        'IN', 'ER'
      )
       OR upper(rs.survey_category_code) = 'ER'
       AND sr.survey_receive_date <= qcp.in_survey_qtr_end_date)
       AND /*** Limitor to Adjusted Sample Indicator (from Press Ganey)  ***/ upper(sr.adjusted_sample_ind) = 'Y'
       AND /*** Limitor to past 4 years  ***/ sr.time_name_child > date_add(DATE(current_ts), interval -48 MONTH)
       AND upper(sq.source_system_code) = 'H'
       AND upper(trim(rs.source_system_code)) = 'H'
       AND upper(sr.source_system_code) = 'H'
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,19, upper(ff2.division_type_code), 21, 22, 23, 24, 25, 26, 27, /*** Group by using Ints and need to add Mode_Adjustment_Amt if applicable ***/ mode_adjustment_amt
      HAVING /***** Response Limitor  - Need at least 7 response in the Denominator (can be lowered with business owner approval to 5) *****/ count(DISTINCT rpd.respondent_id) > 6
    UNION ALL
    SELECT
        -- -GROUP
        dt.qtr_id,
        dt.qtr_desc_dss,
        ff2.corporate_code,
        ff2.corporate_type_code,
        ff2.corporate_name,
        ff2.group_code,
        ff2.group_type_code,
        ff2.group_name,
        '99999' AS division_code,
        CAST(NULL as STRING) AS division_type_code,
        CAST(NULL as STRING) AS division_name,
        '99999' AS market_code,
        CAST(NULL as STRING) AS market_type_code,
        CAST(NULL as STRING) AS market_name,
        ff2.company_code,
        '99999' AS coid,
        CAST(NULL as STRING) AS coid_type_code,
        CAST(NULL as STRING) AS coid_name,
        '99999' AS vendor_assigned_unit_text,
        max(ff2.group_type_code) AS org_level_code,
        rs.survey_category_code,
        psd.domain_id,
        psd.domain_desc,
        sq.measure_id_text,
        sq.question_id,
        sq.question_short_name,
        sq.question_desc,
        sum(CASE
          WHEN qx.domain_id IN(
            75681, 82303, 50010
          )
           AND sr.response_value_text = '1' THEN 1
          ELSE sr.top_box_score_num
        END) AS aggregated_top_box_score_num,
        count(rpd.respondent_id) AS survey_response_cnt,
        count(DISTINCT rpd.respondent_id) AS respondent_cnt,
        sum(CASE
          WHEN qx.domain_id IN(
            75681, 82303, 50010
          )
           AND sr.response_value_text = '1' THEN 1
          ELSE sr.top_box_score_num
        END) / CAST(count(rpd.respondent_id) as NUMERIC) * 100 - CASE
          WHEN mode_adjustment_amt IS NOT NULL THEN mode_adjustment_amt
          ELSE 0
        END AS top_box_pct,
        'H' AS source_system_code,
        current_ts AS dw_last_update_date_time
      FROM
        {{ params.param_hr_base_views_dataset_name }}.survey_response AS sr
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.survey_question AS sq ON sr.survey_question_sid = sq.survey_question_sid
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.respondent_patient_detail AS rpd ON rpd.respondent_id = sr.respondent_id
         AND rpd.survey_receive_date = sr.survey_receive_date
         AND rpd.survey_sid = sq.survey_sid
        INNER JOIN {{ params.param_pub_views_dataset_name }}.lu_date AS dt ON rpd.discharge_date = dt.date_id
        INNER JOIN (
          SELECT
              lu_date.qtr_desc,
              lu_date.qtr_id,
              max(lu_date.date_id) AS eoq_date,
              max(lu_date.date_id) + 42 AS in_survey_qtr_end_date,
              max(lu_date.date_id) + 63 AS oas_survey_qtr_end_date
            FROM
              {{ params.param_pub_views_dataset_name }}.lu_date
            WHERE lu_date.year_id <= 3000
            GROUP BY 1, 2
        ) AS qcp ON qcp.qtr_id = dt.qtr_id
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.patient_satisfaction_alt_org_hierarchy AS alt ON alt.coid = rpd.coid
         AND alt.company_code = rpd.company_code
        INNER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS ff ON alt.coid = ff.coid
         AND alt.company_code = ff.company_code
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.fixed_respondent_patient_detail AS frpd ON frpd.respondent_id = sr.respondent_id
         AND frpd.survey_sid = sq.survey_sid
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_phone_mode_adjustment AS pm ON sq.measure_id_text = pm.measure_id_text
         AND sr.time_name_child BETWEEN pm.eff_from_date AND pm.eff_to_date
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.question_legacy_survey_xwalk AS qx ON qx.question_id = sq.question_id
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_patient_satisfaction_domain AS psd ON psd.domain_id = qx.domain_id
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_survey AS rs ON rs.survey_sid = sq.survey_sid
        INNER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS ff2 ON alt.parent_coid = ff2.coid
      WHERE /*** Include only domain_ids where domain_id is not null or zero ***/ psd.domain_id > 0
       AND /*** For IN, use CMS_Submit_Ind = 'Y' if closed quarter. Use CMS_Submit_Preliminary_Ind if open quarter. For AS, use CMS_Submit_Preliminary_Ind. Else return***/ /*** Only include ER surveys that were received within 42 days after the end of the quarter ***/ (upper(rs.survey_category_code) = 'IN'
       AND upper(sr.cms_submit_ind) = 'Y'
       AND qcp.in_survey_qtr_end_date <  DATE(current_ts)
       OR upper(rs.survey_category_code) = 'IN'
       AND upper(sr.cms_submit_preliminary_ind) = 'Y'
       AND qcp.in_survey_qtr_end_date >=  DATE(current_ts)
       OR rs.survey_category_code NOT IN(
        'IN', 'ER'
      )
       OR upper(rs.survey_category_code) = 'ER'
       AND sr.survey_receive_date <= qcp.in_survey_qtr_end_date)
       AND /*** Limitor to Adjusted Sample Indicator (from Press Ganey)  ***/ upper(sr.adjusted_sample_ind) = 'Y'
       AND /*** Limitor to past 4 years  ***/ sr.time_name_child > date_add(DATE(current_ts), interval -48 MONTH)
       AND upper(sq.source_system_code) = 'H'
       AND upper(trim(rs.source_system_code)) = 'H'
       AND upper(sr.source_system_code) = 'H'
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,19, upper(ff2.group_type_code), 21, 22, 23, 24, 25, 26, 27, /*** Group by using Ints and need to add Mode_Adjustment_Amt if applicable ***/ mode_adjustment_amt
      HAVING /***** Response Limitor  - Need at least 7 response in the Denominator (can be lowered with business owner approval to 5) *****/ count(DISTINCT rpd.respondent_id) > 6
    UNION ALL
    SELECT
        -- -COMPANY
        dt.qtr_id,
        dt.qtr_desc_dss,
        ff2.corporate_code,
        ff2.corporate_type_code,
        ff2.corporate_name,
        '99999' AS group_code,
        CAST(NULL as STRING) AS group_type_code,
        CAST(NULL as STRING) AS group_name,
        '99999' AS division_code,
        CAST(NULL as STRING) AS division_type_code,
        CAST(NULL as STRING) AS division_name,
        '99999' AS market_code,
        CAST(NULL as STRING) AS market_type_code,
        CAST(NULL as STRING) AS market_name,
        ff2.company_code,
        '99999' AS coid,
        CAST(NULL as STRING) AS coid_type_code,
        CAST(NULL as STRING) AS coid_name,
        '99999' AS vendor_assigned_unit_text,
        max(ff2.corporate_type_code) AS org_level_code,
        rs.survey_category_code,
        psd.domain_id,
        psd.domain_desc,
        sq.measure_id_text,
        sq.question_id,
        sq.question_short_name,
        sq.question_desc,
        sum(CASE
          WHEN qx.domain_id IN(
            75681, 82303, 50010
          )
           AND sr.response_value_text = '1' THEN 1
          ELSE sr.top_box_score_num
        END) AS aggregated_top_box_score_num,
        count(rpd.respondent_id) AS survey_response_cnt,
        count(DISTINCT rpd.respondent_id) AS respondent_cnt,
        sum(CASE
          WHEN qx.domain_id IN(
            75681, 82303, 50010
          )
           AND sr.response_value_text = '1' THEN 1
          ELSE sr.top_box_score_num
        END) / CAST(count(rpd.respondent_id) as NUMERIC) * 100 - CASE
          WHEN mode_adjustment_amt IS NOT NULL THEN mode_adjustment_amt
          ELSE 0
        END AS top_box_pct,
        'H' AS source_system_code,
        current_ts AS dw_last_update_date_time
      FROM
        {{ params.param_hr_base_views_dataset_name }}.survey_response AS sr
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.survey_question AS sq ON sr.survey_question_sid = sq.survey_question_sid
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.respondent_patient_detail AS rpd ON rpd.respondent_id = sr.respondent_id
         AND rpd.survey_receive_date = sr.survey_receive_date
         AND rpd.survey_sid = sq.survey_sid
        INNER JOIN {{ params.param_pub_views_dataset_name }}.lu_date AS dt ON rpd.discharge_date = dt.date_id
        INNER JOIN (
          SELECT
              lu_date.qtr_desc,
              lu_date.qtr_id,
              max(lu_date.date_id) AS eoq_date,
              max(lu_date.date_id) + 42 AS in_survey_qtr_end_date,
              max(lu_date.date_id) + 63 AS oas_survey_qtr_end_date
            FROM
              {{ params.param_pub_views_dataset_name }}.lu_date
            WHERE lu_date.year_id <= 3000
            GROUP BY 1, 2
        ) AS qcp ON qcp.qtr_id = dt.qtr_id
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.patient_satisfaction_alt_org_hierarchy AS alt ON alt.coid = rpd.coid
         AND alt.company_code = rpd.company_code
        INNER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS ff ON alt.coid = ff.coid
         AND alt.company_code = ff.company_code
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.fixed_respondent_patient_detail AS frpd ON frpd.respondent_id = sr.respondent_id
         AND frpd.survey_sid = sq.survey_sid
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_phone_mode_adjustment AS pm ON sq.measure_id_text = pm.measure_id_text
         AND sr.time_name_child BETWEEN pm.eff_from_date AND pm.eff_to_date
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.question_legacy_survey_xwalk AS qx ON qx.question_id = sq.question_id
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_patient_satisfaction_domain AS psd ON psd.domain_id = qx.domain_id
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_survey AS rs ON rs.survey_sid = sq.survey_sid
        INNER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS ff2 ON alt.parent_coid = ff2.coid
      WHERE /*** Include only domain_ids where domain_id is not null or zero ***/ psd.domain_id > 0
       AND /*** For IN, use CMS_Submit_Ind = 'Y' if closed quarter. Use CMS_Submit_Preliminary_Ind if open quarter. For AS, use CMS_Submit_Preliminary_Ind. Else return***/ /*** Only include ER surveys that were received within 42 days after the end of the quarter ***/ (upper(rs.survey_category_code) = 'IN'
       AND upper(sr.cms_submit_ind) = 'Y'
       AND qcp.in_survey_qtr_end_date <  DATE(current_ts)
       OR upper(rs.survey_category_code) = 'IN'
       AND upper(sr.cms_submit_preliminary_ind) = 'Y'
       AND qcp.in_survey_qtr_end_date >=  DATE(current_ts)
       OR rs.survey_category_code NOT IN(
        'IN', 'ER'
      )
       OR upper(rs.survey_category_code) = 'ER'
       AND sr.survey_receive_date <= qcp.in_survey_qtr_end_date)
       AND /*** Limitor to Adjusted Sample Indicator (from Press Ganey)  ***/ upper(sr.adjusted_sample_ind) = 'Y'
       AND /*** Limitor to past 4 years  ***/ sr.time_name_child > date_add(DATE(current_ts), interval -48 MONTH)
       AND upper(sq.source_system_code) = 'H'
       AND upper(trim(rs.source_system_code)) = 'H'
       AND upper(sr.source_system_code) = 'H'
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,19, upper(ff2.corporate_type_code), 21, 22, 23, 24, 25, 26, 27, /*** Group by using Ints and need to add Mode_Adjustment_Amt if applicable ***/ mode_adjustment_amt
      HAVING /***** Response Limitor  - Need at least 7 response in the Denominator (can be lowered with business owner approval to 5) *****/ count(DISTINCT rpd.respondent_id) > 6
  ;
  /* Test Unique Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select Qtr_Id , Corporate_Code ,Group_Code ,Division_Code ,Market_Code ,Company_Code , Coid ,Vendor_Assigned_Unit_Text ,Question_Id 
        from {{ params.param_hr_core_dataset_name  }}.bi_psat_question_level_score_smry
        group by Qtr_Id ,Corporate_Code ,Group_Code ,Division_Code ,Market_Code ,Company_Code ,Coid ,Vendor_Assigned_Unit_Text ,Question_Id 
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: edwhr.bi_psat_question_level_score_smry');
    ELSE
      COMMIT TRANSACTION;
    END IF;



END;