UPDATE {{ params.param_psc_audit_dataset_name }}.{{ params.param_control_table_name }} ctrl 
SET last_load_ctrl_timestamp = COALESCE(
(SELECT CAST(FORMAT_DATETIME("%F %R:%E3S", MAX(dwlastupdatedatetime)) AS DATETIME)
FROM {{ params.param_psc_core_dataset_name }}.{{ params.param_load_table_name }}), last_load_ctrl_timestamp)
WHERE ctrl.table_name = '{{ params.param_load_table_name }}';