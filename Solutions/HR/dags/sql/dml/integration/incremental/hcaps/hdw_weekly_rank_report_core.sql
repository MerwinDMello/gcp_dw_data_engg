BEGIN
    DECLARE DUP_COUNT INT64;
    TRUNCATE TABLE {{ params.param_hr_core_dataset_name }}.bi_psat_weekly_rank_report;
    BEGIN TRANSACTION;
        INSERT INTO
        {{ params.param_hr_core_dataset_name }}.bi_psat_weekly_rank_report (rank_report_sk,
            report_type_desc,
            report_data_level,
            group_name,
            division_name,
            market_name,
            facility_name,
            facility_id,
            report_service_line_desc,
            quarter_start_date,
            total_completed_surveys_amt,
            crnt_top_box_pct_num,
            pr_top_box_pct_num,
            crnt_vs_pr_top_box_chng_amt,
            pr_2_top_box_pct_num,
            pr_3_top_box_pct_num,
            pr_yr_top_box_pct_num,
            crnt_vs_pr_yr_top_box_chng_amt,
            roll_top_box_pct_num,
            crnt_vs_roll_top_box_chng_amt,
            hca_rank_total_facility_amt,
            crnt_hca_rank_num,
            pr_hca_rank_num,
            crnt_vs_pr_hca_rank_chng_amt,
            pg_total_facility_amt,
            pg_rank_pct_num,
            report_date,
            benchmark_start_date,
            source_system_code,
            dw_last_update_date_time)
        SELECT
        (
        SELECT
            COALESCE(MAX(rank_report_sk), CAST(0 AS NUMERIC))
        FROM
            {{ params.param_hr_base_views_dataset_name }}.bi_psat_weekly_rank_report ) 
        + CAST(ROW_NUMBER() OVER (ORDER BY src.facility_id) AS NUMERIC) AS rank_report_sk,
        src.report_type_desc,
        src.report_data_level,
        src.group_name,
        IFNULL(src.division_name,'') as division_name,
        IFNULL(src.market_name,'') as market_name,
        IFNULL(SUBSTR(src.facility_name,1,55),'') as facility_name,
        src.facility_id,
        src.report_service_line_desc,
        parse_date('%m/%d/%Y',REGEXP_EXTRACT(src.quarter_start_date, r'[0-9]+/[0-9]+/[0-9]+')) AS quarter_start_date,
        CAST(NULLIF(src.total_completed_surveys_amt,'') AS INT64) AS total_completed_surveys_amt,
        ROUND(CAST(NULLIF(NULLIF(src.crnt_top_box_pct_num,''),'-999') AS NUMERIC),1,"ROUND_HALF_EVEN") AS crnt_top_box_pct_num,
        ROUND(CAST(NULLIF(NULLIF(src.pr_top_box_pct_num,''),'-999') AS NUMERIC),1,"ROUND_HALF_EVEN") AS pr_top_box_pct_num,
        ROUND(CAST(NULLIF(src.crnt_vs_pr_top_box_chng_amt,'') AS NUMERIC),1,"ROUND_HALF_EVEN") AS crnt_vs_pr_top_box_chng_amt,
        ROUND(CAST(NULLIF(src.pr_2_top_box_pct_num,'') AS NUMERIC),1,"ROUND_HALF_EVEN") AS pr_2_top_box_pct_num,
        ROUND(CAST(NULLIF(src.pr_3_top_box_pct_num,'') AS NUMERIC),1,"ROUND_HALF_EVEN") AS pr_3_top_box_pct_num,
        ROUND(CAST(NULLIF(NULLIF(src.pr_yr_top_box_pct_num,''),'-999') AS NUMERIC),1,"ROUND_HALF_EVEN") AS pr_yr_top_box_pct_num,
        ROUND(CAST(NULLIF(src.crnt_vs_pr_yr_top_box_chng_amt,'') AS NUMERIC),1,"ROUND_HALF_EVEN") AS crnt_vs_pr_yr_top_box_chng_amt,
        ROUND(CAST(NULLIF(NULLIF(src.roll_top_box_pct_num,''),'-999') AS NUMERIC),1,"ROUND_HALF_EVEN") AS roll_top_box_pct_num,
        ROUND(CAST(NULLIF(src.crnt_vs_roll_top_box_chng_amt,'') AS NUMERIC),1,"ROUND_HALF_EVEN") AS crnt_vs_roll_top_box_chng_amt,
        CAST(NULLIF(src.hca_rank_total_facility_amt,'') AS INT64) AS hca_rank_total_facility_amt,
        CAST(NULLIF(src.crnt_hca_rank_num,'') AS INT64) AS crnt_hca_rank_num,
        CAST(NULLIF(src.pr_hca_rank_num,'') AS INT64) AS pr_hca_rank_num,
        CAST(NULLIF(src.crnt_vs_pr_hca_rank_chng_amt,'') AS INT64) AS crnt_vs_pr_hca_rank_chng_amt,
        CAST(NULLIF(src.pg_total_facility_amt,'') AS INT64) AS pg_total_facility_amt,
        CAST(NULLIF(src.pg_rank_pct_num,'') AS INT64) AS pg_rank_pct_num,
        parse_date('%m/%d/%Y',REGEXP_EXTRACT(src.report_date, r'[0-9]+/[0-9]+/[0-9]+')) AS report_date,
        parse_date('%m/%d/%Y',REGEXP_EXTRACT(src.benchmark_start_date, r'[0-9]+/[0-9]+/[0-9]+')) AS benchmark_start_date,
        '' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
        FROM
        {{ params.param_hr_stage_dataset_name }}.bi_psat_weekly_rank_report AS src ;

    SET DUP_COUNT = (
        SELECT COUNT(*) FROM(
        SELECT rank_report_sk
        FROM {{ params.param_hr_core_dataset_name }}.bi_psat_weekly_rank_report
        GROUP BY rank_report_sk
        HAVING COUNT(*) > 1
        )
    );

    IF DUP_COUNT <> 0 THEN
        ROLLBACK TRANSACTION;
        RAISE USING MESSAGE = CONCAT(' Duplicates not allowed in table: ' , '{{ params.param_hr_core_dataset_name }}' , '.bi_psat_weekly_rank_report ');
    ELSE 
        COMMIT TRANSACTION;
    END IF
    ;
END
  ;