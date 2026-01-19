UPDATE `{{ params.param_hr_core_dataset_name }}.employee_goal_detail`
SET goal_category_id = 
CASE 
WHEN goal_category_id = 7 THEN 3
WHEN goal_category_id = 9 THEN 4
WHEN goal_category_id = 10 THEN 5
WHEN goal_category_id IN (12,13) THEN 6
END
WHERE goal_category_id IN (7, 9, 10, 12, 13);