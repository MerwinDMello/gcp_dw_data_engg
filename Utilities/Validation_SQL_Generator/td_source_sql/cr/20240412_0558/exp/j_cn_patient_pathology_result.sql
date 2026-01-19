
SELECT	'J_CN_PATIENT_PATHOLOGY_RESULT'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
Select
CN_Patient_Pathology_Res_SID
,Pathology_Result_Type_Id
,Core_Record_Type_Id
,Nav_Patient_Id
,Tumor_Type_Id
,Diagnosis_Result_Id
,Nav_Diagnosis_Id
,Navigator_Id
,Coid
,Company_Code
,Result_Value_Text
,Hashbite_SSK
,Source_System_Code 
,Current_timestamp(0) as DW_Last_Update_Date_Time
from edwcr_staging.CN_PATIENT_PATHOLOGY_RESULT_STG 
where Hashbite_SSK  not in (Select Hashbite_SSK from edwcr.CN_PATIENT_PATHOLOGY_RESULT)
) A;