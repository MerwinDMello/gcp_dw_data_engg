
SELECT	'J_REF_DIAGNOSIS_DETAIL'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
SELECT 
trim(Diagnosis_Detail_Desc) as Diagnosis_Detail_Desc,
trim(Diagnosis_Indicator_Text) as Diagnosis_Indicator_Text,
Source_System_Code as Source_System_Code
FROM EDWCR_Staging.Ref_Diagnosis_Detail_Stg   
where (trim(Diagnosis_Detail_Desc),trim(Diagnosis_Indicator_Text)) not in (sel trim(Diagnosis_Detail_Desc),trim(Diagnosis_Indicator_Text) from EDWCR.Ref_Diagnosis_Detail )
and DW_Last_Update_Date_Time < (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_REF_DIAGNOSIS_DETAIL')
) A;