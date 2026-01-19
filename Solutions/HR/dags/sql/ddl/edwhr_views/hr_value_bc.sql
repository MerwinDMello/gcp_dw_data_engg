
  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.hr_value_bc AS SELECT
      a.employee_num,
      a.eff_from,
      CASE
        WHEN lead(a.eff_from, 1) OVER (PARTITION BY a.employee_num, a.lawson_element_num ORDER BY a.lawson_element_num, a.eff_from) - 1 IS NULL THEN parse_date('%Y-%m-%d', '9999-12-31')
        ELSE lead(a.eff_from, 1) OVER (PARTITION BY a.employee_num, a.lawson_element_num ORDER BY a.lawson_element_num, a.eff_from) - 1
      END AS eff_to,
      a.lawson_element_num,
      a.lawson_element_desc,
      a.lawson_element_value
    FROM
      (
        SELECT
            hreh.employee_num,
            hreh.hr_employee_element_date AS eff_from,
            hreh.lawson_element_num,
            rle.lawson_element_desc,
            CASE
              WHEN upper(rle.lawson_element_type_flag) = 'N' THEN cast(hreh.hr_employee_value_num as string)
              WHEN upper(rle.lawson_element_type_flag) = 'D' THEN cast(hreh.hr_employee_value_date as string)
              ELSE hreh.hr_employee_value_alphanumeric_text
            END AS lawson_element_value,
            hreh.sequence_num,
            row_number() OVER (PARTITION BY hreh.employee_num, hreh.lawson_element_num, hreh.hr_employee_element_date ORDER BY hreh.hr_employee_element_date, hreh.sequence_num DESC) AS row_count
          FROM
            {{ params.param_hr_base_views_dataset_name }}.hr_employee_history AS hreh
            INNER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_lawson_element AS rle ON hreh.lawson_element_num = rle.lawson_element_num
          WHERE date(hreh.valid_to_date) = '9999-12-31'
           AND hreh.lawson_element_num IN(
            2, 3, 4, 5, 156, 6, 7, 8, 9, 59, 1611, 729
          )
          QUALIFY row_count = 1
      ) AS a
      INNER JOIN -- ------ FILTERED THE VALUES USED IN BELOW INSERT (IN JOINS)------
      {{ params.param_pub_views_dataset_name }}.lu_date AS lud ON lud.date_id = current_date()
    WHERE a.row_count = 1
    QUALIFY eff_to >= CAST(concat(CAST(lud.year_id - 2 as STRING), '-01-01') as DATE)
  ;

-- HDM-2040;
