/* Test Unique Index constraint set in Teradata */
SET DUP_COUNT = (
	SELECT COUNT(*)
	FROM (
	SELECT
	Candidate_Onboarding_SID, Valid_From_Date
	from {{ params.param_hr_core_dataset_name }}.candidate_onboarding
	Group BY Candidate_Onboarding_SID, Valid_From_Date
	HAVING COUNT(*) > 1
	)
);
IF DUP_COUNT <> 0 THEN
	ROLLBACK TRANSACTION;
	RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.candidate_onboarding');
ELSE
	COMMIT TRANSACTION;
END IF;