
SELECT CONCAT(count(*), ',	') AS SOURCE_STRING
FROM
 (SELECT Imaging_Mode_Desc,
 Source_System_Code
 FROM EDWCR_Staging.Ref_Imaging_Mode_Stg
 WHERE trim(Imaging_Mode_Desc) NOT IN (sel trim(Imaging_Mode_Desc)
 FROM EDWCR.Ref_Imaging_Mode)
 AND DW_Last_Update_Date_Time <
 (SELECT MAX(Job_Start_Date_Time) AS Job_Start_Date_Time
 FROM EDWCR_DMX_AC.ETL_JOB_RUN
 WHERE Job_Name = 'J_REF_IMAGING_MODE') ) A;