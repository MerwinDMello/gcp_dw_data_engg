-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/payor_current_eom.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.payor_current_eom AS WITH a AS (
  SELECT
      row_number() OVER () AS rn,
      a.*
    FROM
      (
        SELECT
            last_day(current_date('US/Central')) AS pe_date,
            fi.coid,
            fi.company_code,
            fd.unit_num,
            fi.sub_payor_group_id,
            fi.iplan_id,
            fi.major_payor_group_id,
            CASE
              WHEN trim(left(r.parent_payor_name, strpos(r.parent_payor_name, '-'))) BETWEEN '0' AND '99999999999' THEN td_sysfnlib.to_number(trim(left(r.parent_payor_name, strpos(r.parent_payor_name, '-') - 1)))
              ELSE CASE
                WHEN fi.iplan_financial_class_code = 2 THEN -1
                ELSE -1 * CAST(fi.iplan_financial_class_code as INT64)
              END
            END AS major_payor_group_unique_num,
            r.payor_id,
            fi.payor_dw_id,
            p.payor_sid,
            fi.plan_desc,
            r.parent_payor_name,
            r.sub_payor_name,
            r.eff_from_date,
            r.eff_to_date,
            fi.iplan_financial_class_code,
            CASE
              WHEN fi.iplan_financial_class_code = 99
               AND fi.iplan_id <> 0
               AND (fi.iplan_id < 9940
               OR fi.iplan_id > 9949) THEN CAST(20 as NUMERIC)
              ELSE fi.iplan_financial_class_code
            END AS calc_rev_cat_financial_class_code,
            fi.meditech_mnemonic,
            CASE
               upper(p.payor_gen02)
              WHEN 'INSURANCE' THEN 'I'
              WHEN 'PATIENT' THEN 'P'
              ELSE CAST(NULL as STRING)
            END AS payor_gen_02_code,
            fi.payer_type_code
          FROM
            `hca-hin-dev-cur-parallon`.edwpf_base_views.facility_iplan AS fi
            LEFT OUTER JOIN (
              SELECT
                  rcom_payor_dimension_eom.payor_dw_id,
                  rcom_payor_dimension_eom.payor_id,
                  non_null_table.eto AS eff_to_date,
                  non_null_table.efro AS eff_from_date,
                  rcom_payor_dimension_eom.parent_payor_name,
                  rcom_payor_dimension_eom.sub_payor_name
                FROM
                  `hca-hin-dev-cur-parallon`.edwpbs_base_views.rcom_payor_dimension_eom
                  CROSS JOIN (
                    SELECT DISTINCT
                        rcom_payor_dimension_eom_0.payor_dw_id AS p1,
                        max(rcom_payor_dimension_eom_0.eff_to_date) AS eto,
                        min(rcom_payor_dimension_eom_0.eff_from_date) AS efro
                      FROM
                        `hca-hin-dev-cur-parallon`.edwpbs_base_views.rcom_payor_dimension_eom AS rcom_payor_dimension_eom_0
                      WHERE rcom_payor_dimension_eom_0.eff_to_date IS NOT NULL
                       AND rcom_payor_dimension_eom_0.eff_to_date >= last_day(date_add(current_date('US/Central'), interval -1 MONTH))
                       AND rcom_payor_dimension_eom_0.eff_from_date <= last_day(date_add(current_date('US/Central'), interval -1 MONTH))
                      GROUP BY 1
                  ) AS non_null_table
                WHERE rcom_payor_dimension_eom.payor_dw_id = non_null_table.p1
                 AND rcom_payor_dimension_eom.eff_to_date = non_null_table.eto
              UNION ALL
              SELECT
                  rcom_payor_dimension_eom.payor_dw_id,
                  rcom_payor_dimension_eom.payor_id,
                  DATE '9999-12-31' AS eff_to_date,
                  DATE '1900-01-01' AS eff_from_date,
                  rcom_payor_dimension_eom.parent_payor_name,
                  rcom_payor_dimension_eom.sub_payor_name
                FROM
                  `hca-hin-dev-cur-parallon`.edwpbs_base_views.rcom_payor_dimension_eom
                  LEFT OUTER JOIN (
                    SELECT DISTINCT
                        rcom_payor_dimension_eom_0.payor_dw_id AS p1
                      FROM
                        `hca-hin-dev-cur-parallon`.edwpbs_base_views.rcom_payor_dimension_eom AS rcom_payor_dimension_eom_0
                      WHERE rcom_payor_dimension_eom_0.eff_to_date IS NOT NULL
                       AND rcom_payor_dimension_eom_0.eff_to_date >= last_day(date_add(current_date('US/Central'), interval -1 MONTH))
                       AND rcom_payor_dimension_eom_0.eff_from_date <= last_day(date_add(current_date('US/Central'), interval -1 MONTH))
                      GROUP BY 1
                  ) AS non_null_match_table ON rcom_payor_dimension_eom.payor_dw_id = non_null_match_table.p1
                WHERE non_null_match_table.p1 IS NULL
                 AND rcom_payor_dimension_eom.eff_to_date IS NULL
            ) AS r ON fi.payor_dw_id = r.payor_dw_id
            LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_payor_dim AS p ON upper(p.payor_member) = upper(r.payor_id)
             AND p.payor_sid IS NOT NULL
            LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.facility_dimension AS fd ON upper(fd.coid) = upper(fi.coid)
      ) AS a
), a_payor AS (
  SELECT
      a.*,
      payor.pe_date AS pe_date_1,
      payor.payor_dw_id AS payor_dw_id_1,
      payor.coid AS coid_1,
      payor.company_code AS company_code_1,
      payor.iplan_id AS iplan_id_1,
      payor.iplan_financial_class_code AS iplan_financial_class_code_1,
      payor.sub_payor_group_id AS sub_payor_group_id_1,
      payor.major_payor_group_id AS major_payor_group_id_1,
      payor.payor_sid AS payor_sid_1,
      payor.payor_id AS payor_id_1,
      payor.unit_num AS unit_num_1,
      payor.plan_desc AS plan_desc_1,
      payor.sub_payor_group_desc,
      payor.major_payor_group_desc,
      payor.payor_gen_02_code AS payor_gen_02_code_1,
      payor.bankrupt_payor_ind,
      payor.auto_payor_ind,
      payor.mcaid_pending_payor_ind,
      payor.major_payor_group_unique_num AS major_payor_group_unique_num_1,
      payor.calc_rev_cat_financial_class_code AS calc_rev_cat_financial_class_code_1,
      payor.meditech_mnemonic AS meditech_mnemonic_1,
      payor.payer_type_code AS payer_type_code_1,
      payor.eff_from_date AS eff_from_date_1,
      payor.eff_to_date AS eff_to_date_1,
      payor.source_system_code,
      payor.dw_last_update_date_time
    FROM
      a
      INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.payor_eom AS payor ON upper(payor.coid) = upper(a.coid)
       AND payor.iplan_id = a.iplan_id
       AND payor.payor_dw_id = a.payor_dw_id
    WHERE payor.pe_date IN(
      SELECT
          max(payor_eom.pe_date)
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_base_views.payor_eom
    )
), payor AS (
  SELECT
      a.*,
      CAST(NULL as DATE) AS null_0,
      CAST(NULL as NUMERIC) AS null_1,
      CAST(NULL as STRING) AS null_2,
      CAST(NULL as STRING) AS null_3,
      CAST(NULL as INT64) AS null_4,
      CAST(NULL as NUMERIC) AS null_5,
      CAST(NULL as STRING) AS null_6,
      CAST(NULL as STRING) AS null_7,
      CAST(NULL as INT64) AS null_8,
      CAST(NULL as STRING) AS null_9,
      CAST(NULL as STRING) AS null_10,
      CAST(NULL as STRING) AS null_11,
      CAST(NULL as STRING) AS null_12,
      CAST(NULL as STRING) AS null_13,
      CAST(NULL as STRING) AS null_14,
      CAST(NULL as STRING) AS null_15,
      CAST(NULL as STRING) AS null_16,
      CAST(NULL as STRING) AS null_17,
      CAST(NULL as INT64) AS null_18,
      CAST(NULL as NUMERIC) AS null_19,
      CAST(NULL as STRING) AS null_20,
      CAST(NULL as STRING) AS null_21,
      CAST(NULL as DATE) AS null_22,
      CAST(NULL as DATE) AS null_23,
      CAST(NULL as STRING) AS null_24,
      CAST(NULL as TIMESTAMP) AS null_25
    FROM
      a
    WHERE a.rn NOT IN(
      SELECT
          a_payor.rn AS rn
        FROM
          a_payor
    )
)
SELECT
    dense_rank() OVER (ORDER BY xx.month_id, xx.payor_dw_id, upper(xx.coid)) AS sid,
    xx.month_id,
    xx.pe_date,
    xx.coid,
    xx.company_code,
    xx.unit_num,
    xx.sub_payor_group_id,
    xx.iplan_id,
    xx.major_payor_group_id,
    xx.major_payor_group_unique_num,
    xx.payor_id,
    ROUND(xx.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
    xx.payor_sid,
    xx.plan_desc,
    xx.major_payor_group_desc,
    xx.sub_payor_group_desc,
    xx.eff_from_date,
    xx.eff_to_date,
    ROUND(xx.iplan_financial_class_code, 0, 'ROUND_HALF_EVEN') AS iplan_financial_class_code,
    ROUND(xx.calc_rev_cat_financial_class_code, 0, 'ROUND_HALF_EVEN') AS calc_rev_cat_financial_class_code,
    xx.meditech_mnemonic,
    xx.month_day_ind,
    xx.payor_gen_02_code,
    xx.bankrupt_payor_ind,
    xx.auto_payor_ind,
    xx.mcaid_pending_payor_ind,
    xx.payer_type_code,
    xx.dw_last_update_date_time
  FROM
    (
      SELECT
          CASE
             format_date('%Y%m', a.pe_date)
            WHEN '' THEN 0
            ELSE CAST(format_date('%Y%m', a.pe_date) as INT64)
          END AS month_id,
          a.pe_date,
          a.coid,
          a.company_code,
          a.unit_num,
          a.sub_payor_group_id,
          a.iplan_id,
          a.major_payor_group_id,
          a.major_payor_group_unique_num,
          a.payor_id,
          a.payor_dw_id,
          a.payor_sid,
          a.plan_desc,
          a.major_payor_group_desc,
          a.sub_payor_group_desc,
          a.eff_from_date,
          a.eff_to_date,
          a.iplan_financial_class_code,
          a.calc_rev_cat_financial_class_code,
          a.meditech_mnemonic,
          'EOM' AS month_day_ind,
          a.payor_gen_02_code,
          a.bankrupt_payor_ind,
          a.auto_payor_ind,
          a.mcaid_pending_payor_ind,
          a.payer_type_code,
          a.dw_last_update_date_time
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_base_views.payor_eom AS a
      UNION ALL
      SELECT
          CASE
             format_date('%Y%m', x.pe_date)
            WHEN '' THEN 0
            ELSE CAST(format_date('%Y%m', x.pe_date) as INT64)
          END AS month_id,
          x.pe_date,
          x.coid,
          x.company_code,
          x.unit_num,
          x.sub_payor_group_id,
          x.iplan_id,
          x.major_payor_group_id,
          CAST(CASE
            WHEN x.calc_rev_cat_financial_class_code <> x.iplan_financial_class_code
             AND x.major_payor_group_unique_num < 0
             AND x.calc_rev_cat_financial_class_code <> 2 THEN -1 * x.calc_rev_cat_financial_class_code
            ELSE CAST(x.major_payor_group_unique_num as NUMERIC)
          END as INT64) AS major_payor_group_unique_num,
          substr(x.payor_id, 1, 50) AS payor_id,
          x.payor_dw_id,
          coalesce(x.payor_sid, 999) AS payor_sid,
          x.plan_desc,
          substr(x.parent_payor_name, 1, 60) AS major_payor_group_desc,
          substr(x.sub_payor_name, 1, 60) AS sub_payor_group_desc,
          x.eff_from_date,
          x.eff_to_date,
          x.iplan_financial_class_code,
          x.calc_rev_cat_financial_class_code,
          x.meditech_mnemonic,
          'EOD' AS month_day_ind,
          x.payor_gen_02_code,
          x.bankrupt_payor_ind,
          x.auto_payor_ind,
          x.mcaid_pending_payor_ind,
          x.payer_type_code,
          CAST(timestamp_trunc(current_timestamp(), SECOND) as DATETIME) AS dw_last_update_date_time
        FROM
          (
            SELECT
                a_payor_payor.pe_date,
                a_payor_payor.coid,
                a_payor_payor.company_code,
                a_payor_payor.unit_num,
                a_payor_payor.sub_payor_group_id,
                a_payor_payor.iplan_id,
                a_payor_payor.major_payor_group_id,
                a_payor_payor.major_payor_group_unique_num,
                a_payor_payor.payor_id,
                a_payor_payor.payor_dw_id,
                a_payor_payor.payor_sid,
                a_payor_payor.plan_desc,
                a_payor_payor.parent_payor_name,
                a_payor_payor.sub_payor_name,
                a_payor_payor.eff_from_date,
                a_payor_payor.eff_to_date,
                a_payor_payor.iplan_financial_class_code,
                a_payor_payor.calc_rev_cat_financial_class_code,
                a_payor_payor.meditech_mnemonic,
                a_payor_payor.payor_gen_02_code,
                a_payor_payor.bankrupt_payor_ind,
                a_payor_payor.auto_payor_ind,
                a_payor_payor.mcaid_pending_payor_ind,
                a_payor_payor.payer_type_code
              FROM
                (
                  SELECT
                      a_payor.pe_date,
                      a_payor.coid,
                      a_payor.company_code,
                      a_payor.unit_num,
                      a_payor.sub_payor_group_id,
                      a_payor.iplan_id,
                      a_payor.major_payor_group_id,
                      a_payor.major_payor_group_unique_num,
                      a_payor.payor_id,
                      a_payor.payor_dw_id,
                      a_payor.payor_sid,
                      a_payor.plan_desc,
                      a_payor.parent_payor_name,
                      a_payor.sub_payor_name,
                      a_payor.eff_from_date,
                      a_payor.eff_to_date,
                      a_payor.iplan_financial_class_code,
                      a_payor.calc_rev_cat_financial_class_code,
                      a_payor.meditech_mnemonic,
                      a_payor.payor_gen_02_code,
                      a_payor.payer_type_code,
                      a_payor.pe_date_1,
                      a_payor.payor_dw_id_1,
                      a_payor.coid_1,
                      a_payor.company_code_1,
                      a_payor.iplan_id_1,
                      a_payor.iplan_financial_class_code_1,
                      a_payor.sub_payor_group_id_1,
                      a_payor.major_payor_group_id_1,
                      a_payor.payor_sid_1,
                      a_payor.payor_id_1,
                      a_payor.unit_num_1,
                      a_payor.plan_desc_1,
                      a_payor.sub_payor_group_desc,
                      a_payor.major_payor_group_desc,
                      a_payor.payor_gen_02_code_1,
                      a_payor.bankrupt_payor_ind,
                      a_payor.auto_payor_ind,
                      a_payor.mcaid_pending_payor_ind,
                      a_payor.major_payor_group_unique_num_1,
                      a_payor.calc_rev_cat_financial_class_code_1,
                      a_payor.meditech_mnemonic_1,
                      a_payor.payer_type_code_1,
                      a_payor.eff_from_date_1,
                      a_payor.eff_to_date_1,
                      a_payor.source_system_code,
                      a_payor.dw_last_update_date_time
                    FROM
                      a_payor
                  UNION ALL
                  SELECT
                      payor.pe_date,
                      payor.coid,
                      payor.company_code,
                      payor.unit_num,
                      payor.sub_payor_group_id,
                      payor.iplan_id,
                      payor.major_payor_group_id,
                      payor.major_payor_group_unique_num,
                      payor.payor_id,
                      payor.payor_dw_id,
                      payor.payor_sid,
                      payor.plan_desc,
                      payor.parent_payor_name,
                      payor.sub_payor_name,
                      payor.eff_from_date,
                      payor.eff_to_date,
                      payor.iplan_financial_class_code,
                      payor.calc_rev_cat_financial_class_code,
                      payor.meditech_mnemonic,
                      payor.payor_gen_02_code,
                      payor.payer_type_code,
                      payor.null_0 AS pe_date_1,
                      payor.null_1 AS payor_dw_id_1,
                      payor.null_2 AS coid_1,
                      payor.null_3 AS company_code_1,
                      payor.null_4 AS iplan_id_1,
                      payor.null_5 AS iplan_financial_class_code_1,
                      payor.null_6 AS sub_payor_group_id_1,
                      payor.null_7 AS major_payor_group_id_1,
                      payor.null_8 AS payor_sid_1,
                      payor.null_9 AS payor_id_1,
                      payor.null_10 AS unit_num_1,
                      payor.null_11 AS plan_desc_1,
                      payor.null_12 AS sub_payor_group_desc,
                      payor.null_13 AS major_payor_group_desc,
                      payor.null_14 AS payor_gen_02_code_1,
                      payor.null_15 AS bankrupt_payor_ind,
                      payor.null_16 AS auto_payor_ind,
                      payor.null_17 AS mcaid_pending_payor_ind,
                      payor.null_18 AS major_payor_group_unique_num_1,
                      payor.null_19 AS calc_rev_cat_financial_class_code_1,
                      payor.null_20 AS meditech_mnemonic_1,
                      payor.null_21 AS payer_type_code_1,
                      payor.null_22 AS eff_from_date_1,
                      payor.null_23 AS eff_to_date_1,
                      payor.null_24 AS source_system_code,
                      payor.null_25 AS dw_last_update_date_time
                    FROM
                      payor
                ) AS a_payor_payor
          ) AS x
    ) AS xx
;
