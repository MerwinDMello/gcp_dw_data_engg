export JOBNAME='J_CN_PATIENT_TUMOR'
export AC_EXP_SQL_STATEMENT="Select 'J_CN_PATIENT_TUMOR'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from 

(
Select 
STG.CN_Patient_Tumor_SID,
STG.Nav_Patient_Id,
STG.Tumor_Type_Id,
STG.Navigator_Id,
STG.Coid,
'H' as Company_Code,
STG.Electronic_Folder_Id_Text,
RF1.Facility_Id as Referral_Source_Facility_Id,
RS.Status_ID as Nav_Status_Id,
STG.Identification_Period_Text,
STG.Referral_Date,
STG.Referring_Physician_Id,
STG.Nav_End_Reason_Text,
STG.Treatment_End_Reason_Text,
PD.Physician_Id as Treatment_End_Physician_Id,
RF2.Facility_Id as Treatment_End_Facility_Id,
STG.Hashbite_SSK,
'N' as Source_System_Code,
Current_Timestamp(0) as DW_Last_Update_Date_Time
from
$NCR_STG_SCHEMA.CN_Patient_Tumor_STG STG
Left outer join $NCR_TGT_SCHEMA.Ref_Facility RF1
on
STG.TumorReferralSource=RF1.Facility_Name

Left outer join $NCR_TGT_SCHEMA.Ref_Status RS
on STG.NavigationStatus = RS.Status_Desc

Left outer join $NCR_TGT_SCHEMA.CN_Physician_Detail PD
on
STG.EndTreatmentPhysician=PD.Physician_Name
And PD.Physician_Phone_Num is null

Left Outer Join $NCR_TGT_SCHEMA.Ref_Facility RF2
On
STG.EndTreatmentLocation=RF2.Facility_Name

qualify row_number() over(partition by CN_Patient_Tumor_SID order by PD.Physician_ID Desc)=1
 where STG.Hashbite_SSK not in ( Select Hashbite_SSK from $NCR_TGT_SCHEMA.CN_Patient_Tumor )
) SRC"



export AC_ACT_SQL_STATEMENT="Select 'J_CN_PATIENT_TUMOR'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from $NCR_TGT_SCHEMA.CN_Patient_Tumor
where DW_LAST_UPDATE_DATE_TIME >= (Select max(Job_Start_Date_time) from $NCR_AC_VIEW.ETL_JOB_RUN where Job_Name='J_CN_PATIENT_TUMOR');"
