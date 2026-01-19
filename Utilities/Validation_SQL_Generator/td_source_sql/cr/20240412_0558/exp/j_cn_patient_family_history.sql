Select 'J_CN_PATIENT_FAMILY_HISTORY'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from 
(
Select 
PatientHistoryFactID as CN_Patient_Family_History_SID,
Family_History_Query_Id,
PatientDimID as  Nav_Patient_Id,
Coid,
'H' as Company_Code,
Family_History_Value_Text,
HBSource as Hashbite_SSK,
'N' as Source_System_Code,
Current_Timestamp(0) as DW_Last_Update_Date_Time
from
edwcr_staging.CN_Patient_Family_History_STG
where HBSource not in ( Select Hashbite_SSK from Edwcr_Base_Views.CN_PATIENT_FAMILY_HISTORY)
) SRC