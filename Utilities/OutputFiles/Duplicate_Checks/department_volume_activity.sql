/* Test Unique Index constraint set in Teradata */
SET DUP_COUNT = (
	SELECT COUNT(*)
	FROM (
	SELECT
	Company_Code, Coid, Dept_Level_Num, Activity_Scenario_Type_Code, Workforce_Type_Code, Workforce_Activity_Type_Code, Performance_Type_Code, Activity_Measurement_Type_Code, Vol_Activity_Type_Code, Eff_From_Date
	from {{ params.param_pi_core_dataset_name }}.department_volume_activity
	Group BY Company_Code, Coid, Dept_Level_Num, Activity_Scenario_Type_Code, Workforce_Type_Code, Workforce_Activity_Type_Code, Performance_Type_Code, Activity_Measurement_Type_Code, Vol_Activity_Type_Code, Eff_From_Date
	HAVING COUNT(*) > 1
	)
);
IF DUP_COUNT <> 0 THEN
	ROLLBACK TRANSACTION;
	RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table: {{ params.param_pi_core_dataset_name }}.department_volume_activity');
ELSE
	COMMIT TRANSACTION;
END IF;