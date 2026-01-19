-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/arr_daily_report_flag.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.arr_daily_report_flag AS SELECT
    dt.date_id,
    dt.week_id,
    dt.week_num,
    dt.week_end_date,
    dt.month_id,
    dt.month_id_desc_s,
    dt.month_id_desc_l,
    dt.month_num,
    dt.month_num_desc_s,
    dt.month_num_desc_l,
    dt.qtr_id,
    dt.qtr_desc,
    dt.qtr_num,
    dt.qtr_num_desc_s,
    dt.qtr_num_desc_l,
    dt.semi_annual_id,
    dt.semi_annual_desc,
    dt.year_id,
    dt.day_of_week,
    dt.dow_desc_s,
    dt.dow_desc_l,
    dt.day_of_month,
    dt.day_of_qtr,
    dt.day_of_year,
    dt.days_in_week,
    dt.days_in_month,
    dt.days_in_qtr,
    dt.days_in_year,
    dt.month_days_left,
    dt.week_days_left,
    dt.qtr_days_left,
    dt.year_days_left,
    dt.week_day_flag,
    dt.week_end_flag,
    dt.federal_bank_holiday_flag,
    dt.hca_holiday_flag,
    dt.pe_date,
    dt.pe_date_prior_month,
    dt.week_of_month,
    dt.bank_day_flag,
    dt.qtr_desc_dss,
    dt.date_prev,
    dt.qtr_num_prev,
    dt.year_id_prev,
    dt.month_id_prev,
    dt.week_id_prev,
    dt.week_num_desc,
    dt.prev_month_day_date,
    dt.prev_qtr_day_date,
    dt.prev_year_day_date,
    dt.business_day,
    dt.business_day_mtd,
    dt.business_day_ytd
  FROM
    (
      SELECT
          max(fact_ar_patient_level_daily.rptg_date) AS rptg_date
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_ar_patient_level_daily
    ) AS fact
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_date AS dt ON dt.date_id = fact.rptg_date
;
