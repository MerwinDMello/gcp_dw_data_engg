
SELECT	'J_CN_PATIENT_PHONE_NUM'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
SELECT 
Nav_Patient_Id                
FROM edwcr_staging.CN_Patient_Phone_Num_stg
where Nav_Patient_Id not in (Select Nav_Patient_Id from edwcr.CN_Patient_Phone_Num)
and DW_Last_Update_Date_Time < (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC_BASE_VIEWS.ETL_JOB_RUN where Job_Name = 'J_CN_PATIENT_PHONE_NUM')
) A;