
SELECT	'J_REF_BREAST_CANCER_TYPE'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
SELECT 
Breast_Cancer_Type_Desc,
Source_System_Code
FROM EdwCR_Staging.Ref_Breast_Cancer_Type_Stg 
where trim(Breast_Cancer_Type_Desc) not in (sel trim(Breast_Cancer_Type_Desc) from EdwCR.Ref_Breast_Cancer_Type )
and DW_Last_Update_Date_Time < (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_REF_BREAST_CANCER_TYPE')
) A;