SELECT CONCAT(COUNT(*)) AS SOURCE_STRING
FROM EDWCR.Ref_NavQue_Action
WHERE DW_Last_Update_Date_Time>=
 (SELECT max(Job_Start_Date_Time)
 FROM EDWCR_DMX_AC.ETL_JOB_RUN
 WHERE Job_Name='J_Ref_NavQue_Action'
 AND Job_Status_Code IS NULL)