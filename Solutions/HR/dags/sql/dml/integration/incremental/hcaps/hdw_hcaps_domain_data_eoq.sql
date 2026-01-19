BEGIN
DECLARE
  dup_count int64;
DECLARE
  current_ts datetime;
SET
  current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);
BEGIN TRANSACTION;
MERGE INTO
  {{ params.param_hr_core_dataset_name }}.patient_satisfaction_domain_data_eoq AS tgt
USING
  (
  SELECT
    x.time_period_name,
    x.company_code,
    x.corporate_code,
    x.group_code,
    x.division_code,
    x.market_code,
    x.parent_coid,
    x.coid,
    x.facility_claim_control_num,
    x.organization_level_text,
    x.domain_id,
    x.respondent_count,
    x.top_box_num,
    x.vendor_assigned_percentile_rank_num,
    x.reporting_period_text,
    'H' AS source_system_code,
    current_ts AS dw_last_update_date_time
  FROM (
    SELECT
      TRIM(REPLACE(domain_stg.time_period, 'Q', '0')) AS time_period_name,
      ff.company_code,
      ff.corporate_code,
      ff.group_code,
      ff.division_code,
      ff.market_code,
      bp.parent_coid,
      ff.coid,
      bp.ccn AS facility_claim_control_num,
      'FACILITY' AS organization_level_text,
      domain_stg.analysis_id AS domain_id,
      domain_stg.n AS respondent_count,
      domain_stg.top_box AS top_box_num,
      domain_stg.rnk AS vendor_assigned_percentile_rank_num,
      MAX(CASE
          WHEN LENGTH(domain_stg.time_period) = 6 THEN 'Quarterly'
          WHEN LENGTH(domain_stg.time_period) = 13 THEN 'Rolling_Four_Quarter'
        ELSE
        CAST(NULL AS STRING)
      END
        ) AS reporting_period_text
    FROM
      {{params.param_hr_stage_dataset_name}}.domn_data_cms_ccn AS domain_stg
    INNER JOIN
      {{ params.param_pub_views_dataset_name }}.cdm_facility_detail AS bp
    ON
      bp.coid = domain_stg.coid
    INNER JOIN
      {{ params.param_pub_views_dataset_name }}.fact_facility AS ff
    ON
      bp.coid = ff.coid
    WHERE
      UPPER(bp.primary_facility_ind) = 'Y'
      AND UPPER(ff.company_code) = 'H'
    GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11,
      12,
      13,
      14,
      upper(CASE
              WHEN length(domain_stg.time_period) = 6 THEN 'Quarterly'
              WHEN length(domain_stg.time_period) = 13 THEN 'Rolling_Four_Quarter'
              ELSE CAST(NULL as STRING)
            END)
    UNION DISTINCT
    SELECT
      TRIM(REPLACE(domain_stg.time_period, 'Q', '0')) AS time_period_name,
      ff.company_code,
      ff.corporate_code,
      '99999' AS group_code,
      '99999' AS division_code,
      '99999' AS market_code,
      '99999' AS parent_coid,
      '99999' AS coid,
      '9999999999' AS facility_claim_control_num,
      'CORPORATE' AS organization_level_text,
      domain_stg.analysis_id AS domain_id,
      domain_stg.n AS respondent_count,
      domain_stg.top_box AS top_box_num,
      domain_stg.rnk AS vendor_assigned_percentile_rank_num,
      MAX(CASE
          WHEN LENGTH(domain_stg.time_period) = 6 THEN 'Quarterly'
          WHEN LENGTH(domain_stg.time_period) = 13 THEN 'Rolling_Four_Quarter'
        ELSE
        CAST(NULL AS STRING)
      END
        ) AS reporting_period_text
    FROM
      {{params.param_hr_stage_dataset_name}}.domn_data_cms_ccn AS domain_stg
    INNER JOIN
      {{ params.param_pub_views_dataset_name }}.fact_facility AS ff
    ON
      domain_stg.lbl = SUBSTR(ff.corporate_name, 1, 3)
      AND UPPER(ff.company_code) = 'H'
    WHERE
      domain_stg.coid IS NULL
      AND domain_stg.ccn IS NULL
    GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11,
      12,
      13,
      14,
      upper(CASE
              WHEN length(domain_stg.time_period) = 6 THEN 'Quarterly'
              WHEN length(domain_stg.time_period) = 13 THEN 'Rolling_Four_Quarter'
              ELSE CAST(NULL as STRING)
            END)
    UNION DISTINCT
    SELECT
      TRIM(REPLACE(domain_stg.time_period, 'Q', '0')) AS time_period_name,
      ff.company_code,
      ff.corporate_code,
      ff.group_code AS group_code,
      '99999' AS division_code,
      '99999' AS market_code,
      '99999' AS parent_coid,
      '99999' AS coid,
      '9999999999' AS facility_claim_control_num,
      MAX(ff.group_type_name) AS organization_level_text,
      domain_stg.analysis_id AS domain_id,
      domain_stg.n AS respondent_count,
      domain_stg.top_box AS top_box_num,
      domain_stg.rnk AS vendor_assigned_percentile_rank_num,
      MAX(CASE
          WHEN LENGTH(domain_stg.time_period) = 6 THEN 'Quarterly'
          WHEN LENGTH(domain_stg.time_period) = 13 THEN 'Rolling_Four_Quarter'
        ELSE
        CAST(NULL AS STRING)
      END
        ) AS reporting_period_text
    FROM
      {{params.param_hr_stage_dataset_name}}.domn_data_cms_ccn AS domain_stg
    INNER JOIN (
      SELECT
        company_code,
        corporate_code,
        group_type_code,
        group_code,
        group_type_name
      FROM
        {{ params.param_pub_views_dataset_name }}.fact_facility
      GROUP BY
        1,
        2,
        3,
        4,
        5 ) AS ff
    ON
      SUBSTR(domain_stg.coid, 1, 1) = ff.group_type_code
      AND UPPER(ff.company_code) = 'H'
      AND CONCAT(SUBSTR('00000', 1, 5 - LENGTH(TRIM(SUBSTR(CONCAT(SUBSTR(domain_stg.coid, 2, LENGTH(domain_stg.coid) - 1), '     '), 1, 5)))), SUBSTR(domain_stg.coid, 2, LENGTH(domain_stg.coid) - 1)) = ff.group_code
    WHERE
      domain_stg.coid IS NOT NULL
      AND domain_stg.ccn IS NULL
    GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      upper(ff.group_type_name),
      11,
      12,
      13,
      14,
      upper(CASE
              WHEN length(domain_stg.time_period) = 6 THEN 'Quarterly'
              WHEN length(domain_stg.time_period) = 13 THEN 'Rolling_Four_Quarter'
              ELSE CAST(NULL as STRING)
            END)
    UNION DISTINCT
    SELECT
      TRIM(REPLACE(domain_stg.time_period, 'Q', '0')) AS time_period_name,
      ff.company_code,
      ff.corporate_code,
      ff.group_code AS group_code,
      ff.division_code AS division_code,
      '99999' AS market_code,
      '99999' AS parent_coid,
      '99999' AS coid,
      '9999999999' AS facility_claim_control_num,
      MAX(ff.division_type_name) AS organization_level_text,
      domain_stg.analysis_id AS domain_id,
      domain_stg.n AS respondent_count,
      domain_stg.top_box AS top_box_num,
      domain_stg.rnk AS vendor_assigned_percentile_rank_num,
      MAX(CASE
          WHEN LENGTH(domain_stg.time_period) = 6 THEN 'Quarterly'
          WHEN LENGTH(domain_stg.time_period) = 13 THEN 'Rolling_Four_Quarter'
        ELSE
        CAST(NULL AS STRING)
      END
        ) AS reporting_period_text
    FROM
      {{params.param_hr_stage_dataset_name}}.domn_data_cms_ccn AS domain_stg
    INNER JOIN (
      SELECT
        company_code,
        corporate_code,
        group_type_code,
        group_code,
        group_type_name,
        division_type_code,
        division_code,
        division_type_name
      FROM
        {{ params.param_pub_views_dataset_name }}.fact_facility
      GROUP BY
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8 ) AS ff
    ON
      SUBSTR(domain_stg.coid, 1, 1) = ff.division_type_code
      AND UPPER(ff.company_code) = 'H'
      AND CONCAT(SUBSTR('00000', 1, 5 - LENGTH(TRIM(SUBSTR(CONCAT(SUBSTR(domain_stg.coid, 2, LENGTH(domain_stg.coid) - 1), '     '), 1, 5)))), SUBSTR(domain_stg.coid, 2, LENGTH(domain_stg.coid) - 1)) = ff.division_code
    WHERE
      domain_stg.coid IS NOT NULL
      AND domain_stg.ccn IS NULL
    GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      upper(ff.division_type_name),
      11,
      12,
      13,
      14,
      upper(CASE
              WHEN length(domain_stg.time_period) = 6 THEN 'Quarterly'
              WHEN length(domain_stg.time_period) = 13 THEN 'Rolling_Four_Quarter'
              ELSE CAST(NULL as STRING)
            END)
    UNION DISTINCT
    SELECT
      TRIM(REPLACE(domain_stg.time_period, 'Q', '0')) AS time_period_name,
      ff.company_code,
      ff.corporate_code,
      ff.group_code AS group_code,
      ff.division_code AS division_code,
      ff.market_code AS market_code,
      '99999' AS parent_coid,
      '99999' AS coid,
      '9999999999' AS facility_claim_control_num,
      MAX(ff.market_type_name) AS organization_level_text,
      domain_stg.analysis_id AS domain_id,
      domain_stg.n AS respondent_count,
      domain_stg.top_box AS top_box_num,
      domain_stg.rnk AS vendor_assigned_percentile_rank_num,
      MAX(CASE
          WHEN LENGTH(domain_stg.time_period) = 6 THEN 'Quarterly'
          WHEN LENGTH(domain_stg.time_period) = 13 THEN 'Rolling_Four_Quarter'
        ELSE
        CAST(NULL AS STRING)
      END
        ) AS reporting_period_text
    FROM
      {{params.param_hr_stage_dataset_name}}.domn_data_cms_ccn AS domain_stg
    INNER JOIN (
      SELECT
        company_code,
        corporate_code,
        group_type_code,
        group_code,
        group_type_name,
        division_type_code,
        division_code,
        division_type_name,
        market_type_code,
        market_code,
        market_type_name
      FROM
        {{ params.param_pub_views_dataset_name }}.fact_facility
      GROUP BY
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11 ) AS ff
    ON
      SUBSTR(domain_stg.coid, 1, 1) = ff.market_type_code
      AND UPPER(ff.company_code) = 'H'
      AND CONCAT(SUBSTR('00000', 1, 5 - LENGTH(TRIM(SUBSTR(CONCAT(SUBSTR(domain_stg.coid, 2, LENGTH(domain_stg.coid) - 1), '     '), 1, 5)))), SUBSTR(domain_stg.coid, 2, LENGTH(domain_stg.coid) - 1)) = ff.market_code
    WHERE
      domain_stg.coid IS NOT NULL
      AND domain_stg.ccn IS NULL
    GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      upper(ff.market_type_name),
      11,
      12,
      13,
      14,
      upper(CASE
              WHEN length(domain_stg.time_period) = 6 THEN 'Quarterly'
              WHEN length(domain_stg.time_period) = 13 THEN 'Rolling_Four_Quarter'
              ELSE CAST(NULL as STRING)
            END) ) AS x ) AS src
ON
  src.time_period_name = tgt.time_period_name
  AND src.company_code = tgt.company_code
  AND src.corporate_code = tgt.corporate_code
  AND src.group_code = tgt.group_code
  AND src.division_code = tgt.division_code
  AND src.market_code = tgt.market_code
  AND src.parent_coid = tgt.parent_coid
  AND src.coid = tgt.coid
  AND src.facility_claim_control_num = tgt.facility_claim_control_num
  AND src.domain_id = tgt.domain_id
  WHEN MATCHED THEN UPDATE SET organization_level_text = src.organization_level_text, respondent_count = cast(src.respondent_count as int64), top_box_num = src.top_box_num, vendor_assigned_percentile_rank_num = src.vendor_assigned_percentile_rank_num, source_system_code = src.source_system_code, dw_last_update_date_time = src.dw_last_update_date_time, reporting_period_text = src.reporting_period_text
  WHEN NOT MATCHED BY TARGET
  THEN
INSERT
  (time_period_name,
    company_code,
    corporate_code,
    group_code,
    division_code,
    market_code,
    parent_coid,
    coid,
    facility_claim_control_num,
    domain_id,
    organization_level_text,
    respondent_count,
    top_box_num,
    vendor_assigned_percentile_rank_num,
    reporting_period_text,
    source_system_code,
    dw_last_update_date_time)
VALUES
  (src.time_period_name, src.company_code, src.corporate_code, src.group_code, src.division_code, src.market_code, src.parent_coid, src.coid, src.facility_claim_control_num, src.domain_id, src.organization_level_text, cast(src.respondent_count as int64), src.top_box_num, src.vendor_assigned_percentile_rank_num, src.reporting_period_text, src.source_system_code, src.dw_last_update_date_time) ;
SET
  dup_count = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
      Time_Period_Name,
      Company_Code,
      Corporate_Code,
      Group_Code,
      Division_Code,
      Market_Code,
      Parent_Coid,
      Coid,
      Facility_Claim_Control_Num,
      Domain_Id
    FROM
      {{ params.param_hr_core_dataset_name }}.patient_satisfaction_domain_data_eoq
    GROUP BY
      Time_Period_Name,
      Company_Code,
      Corporate_Code,
      Group_Code,
      Division_Code,
      Market_Code,
      Parent_Coid,
      Coid,
      Facility_Claim_Control_Num,
      Domain_Id
    HAVING
      COUNT(*) > 1 ) );
IF
  dup_count <> 0 THEN
ROLLBACK TRANSACTION; raise
USING
  message = CONCAT('duplicates are not allowed in the table: edwhr.patient_satisfaction_domain_data_eoq');
  ELSE
COMMIT TRANSACTION;
END IF
  ;
END
  ;