DELETE FROM hca-hin-prod-cur-hr.edwhr_staging.pay_code_pay_summary_group_crosswalk_stg 
WHERE   kronos_clock_library='CLOC0PHY1'
AND pay_code='TAP'
AND pay_code_desc='TAP';
