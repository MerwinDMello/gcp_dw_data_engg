DELETE
FROM
`{{ params.param_hr_core_dataset_name }}.address`
WHERE (addr_line_1_text, city_name, zip_code, source_system_code)
IN
(
SELECT AS STRUCT addr_line_1_text, city_name, zip_code, source_system_code
FROM
(Select addr_line_1_text, city_name, zip_code, source_system_code
FROM `{{ params.param_hr_core_dataset_name }}.address`
Where source_system_code = 'M'
AND (UPPER(TRIM(addr_line_1_text)) = ''
OR UPPER(TRIM(city_name)) = ''
OR UPPER(TRIM(zip_code)) = ''
)
EXCEPT DISTINCT
Select addr_line_1_text, city_name, zip_code, source_system_code
FROM `hca-hin-dev-cur-hr.edwhr_base_views.address`
Where source_system_code = 'M'))