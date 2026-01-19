Select 'J_CN_PATIENT_IMAGING'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from 
(
Select 
STG.CN_Patient_Imaging_SID,
STG.Core_Record_Type_Id,
STG.Nav_Patient_Id,
STG.Med_Spcl_Physician_Id,
STG.Tumor_Type_Id,
STG.Diagnosis_Result_Id,
STG.Nav_Diagnosis_Id,
STG.Navigator_Id,
STG.Coid,
'H' as Company_Code,
STG.Imaging_Type_Id,
STG.Imaging_Date,
RIM.Imaging_Mode_Id as Imaging_Mode_Id,
RS.Side_Id as Imaging_Area_Side_Id,
STG.Imaging_Location_Text,
RF.Facility_Id as Imaging_Facility_Id,
CASE WHEN (STG.Birad_Scale_Code)='Results not available'  THEN NULL else STG.Birad_Scale_Code END as Birad_Scale_Code,
--STG.Birad_Scale_Code,
STG.Comment_Text,
DS.Status_Id AS Disease_Status_Id,
TS.Status_Id AS Treatment_Status_Id,
STG.Other_Image_Type_Text,
CASE When Initial_Diagnosis_Ind = 'Yes' Then 'Y' When Initial_Diagnosis_Ind = 'No' Then 'N' Else 'U' End AS Initial_Diagnosis_Ind,
CASE When Disease_Monitoring_Ind = 'Yes' Then 'Y' When Disease_Monitoring_Ind = 'No' Then 'N' Else 'U' End AS Disease_Monitoring_Ind,
STG.Radiology_Result_Text,
STG.Hashbite_SSK,
'N' Source_System_Code,
Current_timestamp(0) as DW_Last_Update_Date_Time
from EDWCR_STAGING.CN_Patient_Imaging_STG STG
Left Outer Join EDWCR_BASE_VIEWS.Ref_Imaging_Mode RIM
On
COALESCE(TRIM(STG.ImageMode),'X')=COALESCE(TRIM(RIM.Imaging_Mode_Desc),'X')
Left Outer Join EDWCR_BASE_VIEWS.Ref_Side RS
On
COALESCE(TRIM(STG.ImageArea),'XX')=COALESCE(TRIM(RS.Side_Desc),'XX')
Left Outer Join EDWCR_BASE_VIEWS.Ref_Facility RF
On
COALESCE(TRIM(STG.ImageCenter),'XXX')=COALESCE(TRIM(RF.Facility_Name),'XXX')
Left Outer Join EDWCR_BASE_VIEWS.Ref_Status DS 
On 
COALESCE(TRIM(STG.Disease_Status),'XXX')=COALESCE(TRIM(DS.Status_Desc),'XXX') 
AND DS.Status_Type_Desc='Disease'
Left Outer Join EDWCR_BASE_VIEWS.Ref_Status TS 
On 
COALESCE(TRIM(STG.Treatment_Status),'XXX')=COALESCE(TRIM(TS.Status_Desc),'XXX') 
AND TS.Status_Type_Desc='Treatment'
where Hashbite_SSK not in ( Select Hashbite_SSK from EDWCR_BASE_VIEWS.CN_Patient_Imaging )
) SRC