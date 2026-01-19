SELECT NEWID() as uuid, 
{{ params.table_id }} as table_id, 
'{{ params.source_sys_name }}' as src_sys_nm,
'{{ params.source_table_name }}' as src_tbl_nm,
'{{ params.target_table_name }}' as tgt_tbl_nm,
'RECORD_COUNT' as audit_type, 
expected_rec_count as expected_value, 
{{ params.actual_value }} as actual_value,
'{{ params.load_start_time }}' as load_start_time, 
'{{ params.load_end_time }}' as load_end_time,
'{{ params.load_run_time }}' as load_run_time,
'{{ params.job_name }}' as job_name, 
convert(varchar(23), CURRENT_TIMESTAMP, 121) as audit_time,
CASE 
WHEN expected_rec_count = {{ params.actual_value }} 
THEN 'PASS' ELSE 'FAIL' 
END AS audit_status
FROM
(
SELECT COUNT(*) as expected_rec_count FROM
(
{{ params.source_query }}
) sub_query1
) sub_query2