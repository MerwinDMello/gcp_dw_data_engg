Select 'J_CN_PATIENT_COMPLICATION'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from 
(
Select 
STG.CN_Patient_Complication_SID,
STG.Nav_Patient_Id,
STG.Core_Record_Type_Id,
STG.Tumor_Type_Id,
STG.Diagnosis_Result_Id,
STG.Nav_Diagnosis_Id,
STG.Navigator_Id,
STG.Coid,
'H' as Company_Code,
STG.Complication_Date,
RTT.Therapy_Type_Id,
STG.Treatment_Stopped_Ind,
RS.Nav_Result_ID as Outcome_Result_Id,
STG.Complication_Text,
STG.Specific_Complication_Text,
STG.Comment_Text,
STG.Hashbite_SSK,
'N' as Source_System_Code,
Current_Timestamp(0) as DW_Last_Update_Date_Time
from
edwcr_staging.CN_Patient_Complication_STG STG
Left Outer Join edwcr_base_views.Ref_Therapy_Type RTT
On
COALESCE(TRIM(STG.AssociateTherapyType),'X')=COALESCE(TRIM(RTT.Therapy_Type_Desc),'X')
Left Outer Join edwcr.Ref_Result RS
On
COALESCE(TRIM(STG.ComplicationOutcome),'XX')=COALESCE(TRIM(RS.Nav_Result_Desc),'XX')
where STG.Hashbite_SSK not in ( Select Hashbite_SSK from edwcr_base_views.CN_Patient_Complication )
) SRC