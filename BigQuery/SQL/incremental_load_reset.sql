-- Get Count of Records which were loaded yesterday

Select dw_last_update_date_time, Count(*)
From
`edwhr_copy.time_entry_pay_code_detail`
Where DATE(dw_last_update_date_time) = '2022-12-31'
AND DATE(VALID_TO_DATE) = '9999-12-31'
GROUP BY 1

-- Output: 277520

-- Delete Records which were loaded yesterday

Delete
From
`edwhr_copy.time_entry_pay_code_detail`
Where DATE(dw_last_update_date_time) = '2022-12-31'
AND DATE(VALID_TO_DATE) = '9999-12-31'

-- Deleted Records: 277520

-- Get Count of Records which were updated yesterday

Select dw_last_update_date_time, Count(*)
From
`edwhr_copy.time_entry_pay_code_detail`
Where DATE(dw_last_update_date_time) = '2022-12-31'
AND DATE(VALID_TO_DATE) <> '9999-12-31'
GROUP BY 1

-- Output: 4774

-- Reset Valid_to_Date Records which were updated yesterday

Update
`edwhr_copy.time_entry_pay_code_detail`
Set VALID_TO_DATE = DATETIME('9999-12-31 23:59:59')
Where DATE(dw_last_update_date_time) = '2022-12-31'
AND DATE(VALID_TO_DATE) <> '9999-12-31'

-- Update Records: 4774