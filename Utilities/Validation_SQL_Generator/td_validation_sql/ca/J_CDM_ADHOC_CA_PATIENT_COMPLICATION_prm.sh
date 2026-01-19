export JOBNAME='J_CDM_ADHOC_CA_PATIENT_COMPLICATION'

export AC_EXP_SQL_STATEMENT="
select '$JOBNAME' || ',' || cast(count(*) as varchar(30)) || ',' as SOURCE_STRING 
FROM (
SELECT  * FROM (
select 
1 as Patient_Complication_SK,
TRIM(EDWCDM.CA_Server.Server_SK) as Server_SK,
trim(A.Patient_Case_SK) as Patient_Case_SK, 
trim(CompUniqueID) AS Source_Complication_Unique_Id,
trim(ComplicationID) AS Complication_Id,
trim(ComplicationOthSp) AS Complication_Other_Text,
trim(sort) AS Source_Sort_Num,
cast(cast(CreateDate as varchar(19)) as timestamp(0)) as Source_Create_Date_Time,
cast(cast(LastUpdate as varchar(19)) as timestamp(0)) as Source_Last_Update_Date_Time,
TRIM(UpdateBy) as Updated_By_3_4_Id,
trim('C') as Source_System_Code,
Current_Timestamp(0) as  DW_Last_Update_Date_Time 
from EDWCDM_STAGING.CardioAccess_Complications_STG stg
Inner Join EDWCDM.CA_Server
on trim(Stg.Full_Server_Nm) =trim( CA_Server.Server_Name)
left outer join 
 ( Select Patient_Case_SK,Source_Patient_Case_Num ,Server_Name From
 EDWCDM.CA_Patient_Case  C
 Inner Join EDWCDM.CA_Server S on
 trim(C.Server_SK) = trim(S.Server_SK)
   ) A
 on trim(Stg.Full_Server_NM) = trim(A.Server_Name)
 and trim(Stg.CaseNumber) = trim(A.Source_Patient_Case_Num))P)Q;"

export AC_ACT_SQL_STATEMENT="
 select '$JOBNAME'  || ','  || cast(COUNT(*) as varchar(30)) ||',' as SOURCE_STRING 
 FROM (Select * from  EDWCDM.CA_PATIENT_COMPLICATION)a;"

