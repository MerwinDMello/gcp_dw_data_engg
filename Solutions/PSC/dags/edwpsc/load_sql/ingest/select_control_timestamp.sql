SELECT last_load_ctrl_timestamp 
FROM {{ params.param_psc_audit_dataset_name }}.{{ params.param_control_table_name }}
WHERE table_name = '{{ params.param_load_table_name }}';