-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/pass_eom_pf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.pass_eom_pf AS SELECT
    a.patient_dw_id,
    a.company_code,
    a.coid,
    a.unit_num,
    a.processing_zone_code,
    a.stack_code,
    a.rptg_period,
    date_sub(date_add(date_add(date_sub(parse_date('%Y%m%d', concat(a.rptg_period, '01')), interval extract(DAY from parse_date('%Y%m%d', concat(a.rptg_period, '01'))) DAY), interval 1 DAY), interval 1 MONTH), interval 1 DAY) AS pe_date,
    a.pat_acct_num,
    a.facility_name,
    CASE
      WHEN session_user() = pn.userid THEN '***'
      ELSE a.patient_full_name
    END AS patient_full_name,
    a.financial_class_code,
    a.financial_class_desc,
    a.patient_type_code,
    a.derived_patient_type_code,
    a.account_status_code,
    a.admission_date,
    a.discharge_date,
    a.final_bill_date,
    a.ar_bill_thru_date,
    a.length_of_stay_days_num,
    CASE
      WHEN a.discharge_date IS NULL THEN 0
      WHEN a.discharge_date >= date_add(CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE), interval 1 MONTH) THEN 0
      WHEN abs(EXTRACT(MONTH FROM make_interval(CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0) - make_interval(CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0))) + 1 > 12 THEN 13
      ELSE abs(EXTRACT(MONTH FROM make_interval(CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0) - make_interval(CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0))) + 1
    END AS acct_age_category,
    CASE
      WHEN CASE
        WHEN a.discharge_date IS NULL THEN 0
        WHEN a.discharge_date >= date_add(CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE), interval 1 MONTH) THEN 0
        WHEN abs(EXTRACT(MONTH FROM make_interval(CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0) - make_interval(CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0))) + 1 > 12 THEN 13
        ELSE abs(EXTRACT(MONTH FROM make_interval(CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0) - make_interval(CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0))) + 1
      END = 0
       AND a.discharge_date IS NULL THEN 'Not Discharged'
      WHEN CASE
        WHEN a.discharge_date IS NULL THEN 0
        WHEN a.discharge_date >= date_add(CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE), interval 1 MONTH) THEN 0
        WHEN abs(EXTRACT(MONTH FROM make_interval(CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0) - make_interval(CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0))) + 1 > 12 THEN 13
        ELSE abs(EXTRACT(MONTH FROM make_interval(CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0) - make_interval(CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0))) + 1
      END = 0
       AND a.discharge_date IS NOT NULL THEN 'Next Mo Disch'
      WHEN CASE
        WHEN a.discharge_date IS NULL THEN 0
        WHEN a.discharge_date >= date_add(CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE), interval 1 MONTH) THEN 0
        WHEN abs(EXTRACT(MONTH FROM make_interval(CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0) - make_interval(CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0))) + 1 > 12 THEN 13
        ELSE abs(EXTRACT(MONTH FROM make_interval(CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0) - make_interval(CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0))) + 1
      END = 1 THEN '0-30'
      WHEN CASE
        WHEN a.discharge_date IS NULL THEN 0
        WHEN a.discharge_date >= date_add(CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE), interval 1 MONTH) THEN 0
        WHEN abs(EXTRACT(MONTH FROM make_interval(CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0) - make_interval(CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0))) + 1 > 12 THEN 13
        ELSE abs(EXTRACT(MONTH FROM make_interval(CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0) - make_interval(CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0))) + 1
      END = 2 THEN '31-60'
      WHEN CASE
        WHEN a.discharge_date IS NULL THEN 0
        WHEN a.discharge_date >= date_add(CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE), interval 1 MONTH) THEN 0
        WHEN abs(EXTRACT(MONTH FROM make_interval(CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0) - make_interval(CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0))) + 1 > 12 THEN 13
        ELSE abs(EXTRACT(MONTH FROM make_interval(CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0) - make_interval(CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0))) + 1
      END = 3 THEN '61-90'
      WHEN CASE
        WHEN a.discharge_date IS NULL THEN 0
        WHEN a.discharge_date >= date_add(CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE), interval 1 MONTH) THEN 0
        WHEN abs(EXTRACT(MONTH FROM make_interval(CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0) - make_interval(CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0))) + 1 > 12 THEN 13
        ELSE abs(EXTRACT(MONTH FROM make_interval(CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0) - make_interval(CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0))) + 1
      END = 4 THEN '91-120'
      WHEN CASE
        WHEN a.discharge_date IS NULL THEN 0
        WHEN a.discharge_date >= date_add(CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE), interval 1 MONTH) THEN 0
        WHEN abs(EXTRACT(MONTH FROM make_interval(CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0) - make_interval(CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0))) + 1 > 12 THEN 13
        ELSE abs(EXTRACT(MONTH FROM make_interval(CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0) - make_interval(CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0))) + 1
      END = 5 THEN '121-150'
      WHEN CASE
        WHEN a.discharge_date IS NULL THEN 0
        WHEN a.discharge_date >= date_add(CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE), interval 1 MONTH) THEN 0
        WHEN abs(EXTRACT(MONTH FROM make_interval(CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0) - make_interval(CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0))) + 1 > 12 THEN 13
        ELSE abs(EXTRACT(MONTH FROM make_interval(CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0) - make_interval(CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0))) + 1
      END = 6 THEN '151-180'
      WHEN CASE
        WHEN a.discharge_date IS NULL THEN 0
        WHEN a.discharge_date >= date_add(CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE), interval 1 MONTH) THEN 0
        WHEN abs(EXTRACT(MONTH FROM make_interval(CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0) - make_interval(CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0))) + 1 > 12 THEN 13
        ELSE abs(EXTRACT(MONTH FROM make_interval(CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0) - make_interval(CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0))) + 1
      END = 7 THEN '181-210'
      WHEN CASE
        WHEN a.discharge_date IS NULL THEN 0
        WHEN a.discharge_date >= date_add(CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE), interval 1 MONTH) THEN 0
        WHEN abs(EXTRACT(MONTH FROM make_interval(CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0) - make_interval(CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0))) + 1 > 12 THEN 13
        ELSE abs(EXTRACT(MONTH FROM make_interval(CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0) - make_interval(CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0))) + 1
      END = 8 THEN '211-240'
      WHEN CASE
        WHEN a.discharge_date IS NULL THEN 0
        WHEN a.discharge_date >= date_add(CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE), interval 1 MONTH) THEN 0
        WHEN abs(EXTRACT(MONTH FROM make_interval(CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0) - make_interval(CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0))) + 1 > 12 THEN 13
        ELSE abs(EXTRACT(MONTH FROM make_interval(CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0) - make_interval(CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0))) + 1
      END = 9 THEN '241-270'
      WHEN CASE
        WHEN a.discharge_date IS NULL THEN 0
        WHEN a.discharge_date >= date_add(CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE), interval 1 MONTH) THEN 0
        WHEN abs(EXTRACT(MONTH FROM make_interval(CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0) - make_interval(CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0))) + 1 > 12 THEN 13
        ELSE abs(EXTRACT(MONTH FROM make_interval(CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0) - make_interval(CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0))) + 1
      END = 10 THEN '271-300'
      WHEN CASE
        WHEN a.discharge_date IS NULL THEN 0
        WHEN a.discharge_date >= date_add(CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE), interval 1 MONTH) THEN 0
        WHEN abs(EXTRACT(MONTH FROM make_interval(CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0) - make_interval(CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0))) + 1 > 12 THEN 13
        ELSE abs(EXTRACT(MONTH FROM make_interval(CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0) - make_interval(CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0))) + 1
      END = 11 THEN '301-330'
      WHEN CASE
        WHEN a.discharge_date IS NULL THEN 0
        WHEN a.discharge_date >= date_add(CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE), interval 1 MONTH) THEN 0
        WHEN abs(EXTRACT(MONTH FROM make_interval(CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0) - make_interval(CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0))) + 1 > 12 THEN 13
        ELSE abs(EXTRACT(MONTH FROM make_interval(CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0) - make_interval(CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0))) + 1
      END = 12 THEN '331-360'
      WHEN CASE
        WHEN a.discharge_date IS NULL THEN 0
        WHEN a.discharge_date >= date_add(CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE), interval 1 MONTH) THEN 0
        WHEN abs(EXTRACT(MONTH FROM make_interval(CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0) - make_interval(CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0))) + 1 > 12 THEN 13
        ELSE abs(EXTRACT(MONTH FROM make_interval(CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(a.discharge_date, '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0) - make_interval(CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 1) as INT64), CAST(regexp_extract(format_date('%Y-%m', CAST(concat(substr(a.rptg_period, 1, 4), '-', substr(a.rptg_period, 5, 2), '-01') as DATE)), '([0-9]+)', 1, 2) as INT64), 0, 0, 0, 0))) + 1
      END = 13 THEN '> 360'
    END AS acct_age_category_desc,
    a.collector_org_code,
    a.collector_org_type_code,
    a.collector_org_short_name,
    a.log_id,
    a.log_name,
    a.ip_contract_model_code,
    a.iplan_id_ins1,
    a.payor_name_ins1,
    a.release_ind_ins1,
    a.payor_balance_amt_ins1,
    a.payor_liability_amt_ins1,
    a.payor_payment_amt_ins1,
    a.payor_adjustment_amt_ins1,
    a.payor_contract_allow_amt_ins1,
    a.pa_syst_adj_current_amt_ins1,
    a.pa_new_actv_current_amt_ins1,
    a.iplan_id_ins2,
    a.payor_name_ins2,
    a.release_ind_ins2,
    a.payor_balance_amt_ins2,
    a.payor_liability_amt_ins2,
    a.payor_payment_amt_ins2,
    a.payor_adjustment_amt_ins2,
    a.payor_contract_allow_amt_ins2,
    a.pa_syst_adj_current_amt_ins2,
    a.pa_new_actv_current_amt_ins2,
    a.iplan_id_ins3,
    a.payor_name_ins3,
    a.release_ind_ins3,
    a.payor_balance_amt_ins3,
    a.payor_liability_amt_ins3,
    a.payor_payment_amt_ins3,
    a.payor_adjustment_amt_ins3,
    a.payor_contract_allow_amt_ins3,
    a.pa_syst_adj_current_amt_ins3,
    a.pa_new_actv_current_amt_ins3,
    a.patient_balance_amt,
    a.patient_liability_amt,
    a.patient_payment_amt,
    a.patient_adjustment_amt,
    a.patient_contract_allow_amt,
    a.total_billed_charge_amt,
    a.total_rb_charge_amt,
    a.total_anc_charge_amt,
    a.total_write_off_bad_debt_amt,
    a.total_write_off_other_amt,
    a.total_adjustment_amt,
    a.total_contract_allow_amt,
    a.total_combined_adj_alw_amt,
    a.total_payment_amt,
    a.total_account_balance_amt,
    a.prelim_account_balance_amt,
    a.eor_gross_reimbursement_amt,
    a.eor_contract_allow_amt,
    a.eor_auto_post_amt,
    a.discharge_to_eom_day_cnt,
    a.current_month_amt,
    a.sub_unit_num,
    a.sub_unit_name,
    a.drg_code,
    a.drg_payment_weight_amt,
    a.drg_code_hcfa,
    a.drg_code_classic,
    a.drg_code_pre_hac,
    a.drg_code_pre_hac_tricare,
    a.denial_code_ins1,
    a.financial_class_code_ins1,
    a.denial_code_ins2,
    a.financial_class_code_ins2,
    a.denial_code_ins3,
    a.financial_class_code_ins3,
    a.prorated_release_amt_ins1,
    a.prorated_release_amt_ins2,
    a.prorated_release_amt_ins3,
    a.prorated_release_date_ins1,
    a.prorated_release_date_ins2,
    a.prorated_release_date_ins3,
    a.logging_ind,
    a.auto_post_ind,
    a.unbill_pre_admit_ind,
    a.insured_employer_name,
    a.insured_role_code,
    a.apr_severity_of_illness,
    a.apr_risk_of_mortality
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.pass_eom AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(b.co_id) = upper(a.coid)
     AND upper(b.company_code) = upper(a.company_code)
     AND b.user_id = session_user()
    LEFT OUTER JOIN (
      SELECT
          security_mask_and_exception.userid,
          security_mask_and_exception.masked_column_code
        FROM
          `hca-hin-dev-cur-parallon`.edw_sec_base_views.security_mask_and_exception
        WHERE security_mask_and_exception.userid = session_user()
         AND upper(security_mask_and_exception.masked_column_code) = 'PN'
    ) AS pn ON pn.userid = session_user()
;
