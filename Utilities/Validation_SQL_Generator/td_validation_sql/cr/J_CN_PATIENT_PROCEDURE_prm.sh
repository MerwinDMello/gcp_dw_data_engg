export JOBNAME='J_CN_PATIENT_PROCEDURE'


export AC_EXP_SQL_STATEMENT="
SELECT	'J_CN_PATIENT_PROCEDURE'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
SELECT
stg.CN_Patient_Procedure_SID
,stg.Core_Record_Type_Id 
,REFPR.Procedure_Type_Id
,stg.Med_Spcl_Physician_Id
,stg.Nav_Patient_Id
,stg.Tumor_Type_Id 
,stg.Diagnosis_Result_Id 
,stg.Nav_Diagnosis_Id 
,stg.Navigator_Id 
,stg.Coid 
,stg.Company_Code 
,stg.Procedure_Date
,stg.Palliative_Ind
,stg.Hashbite_SSK 
,stg.Source_System_Code 
,Current_timestamp(0) as DW_Last_Update_Date_Time
from edwcr_staging.CN_Patient_Procedure_Stg stg
Left JOIN EDWCR_Base_Views.Ref_Procedure_Type REFPR
ON stg.ProcedureType=REFPR.Procedure_Type_Desc
AND Coalesce(stg.OtherProcedureType ,stg.OtherSurgeryType ,stg.LinePlacementType,'XX')= Coalesce(REFPR.Procedure_Sub_Type_Desc,'XX')

where trim(stg.Hashbite_SSK) not in ( Select trim(Hashbite_SSK) from EDWCR_Base_Views.CN_PATIENT_PROCEDURE ) 

) A;"


export AC_ACT_SQL_STATEMENT="select 'J_CN_PATIENT_PROCEDURE'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM $NCR_TGT_SCHEMA.CN_PATIENT_PROCEDURE WHERE 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_CN_PATIENT_PROCEDURE');"



