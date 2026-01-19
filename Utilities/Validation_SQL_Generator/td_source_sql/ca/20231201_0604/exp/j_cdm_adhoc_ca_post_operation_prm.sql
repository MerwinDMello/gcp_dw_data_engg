
select '$JOBNAME' || ',' || cast(count(*) as varchar(30)) || ',' as SOURCE_STRING 
FROM (
SELECT  * FROM (
select 
1 as Post_Operation_SK,
TRIM(EDWCDM.CA_Server.Server_SK) as Server_SK,
trim(A.Patient_Case_SK) as Patient_Case_SK,
trim(PostOpID) as Source_Post_Operation_Id,
trim(Intubate) as Intubate_Id,
cast(cast(IntubateDT as varchar(19)) as timestamp(0)) as Intubate_Date_Time,
cast(cast(ExtubateDT as varchar(19)) as timestamp(0)) as Extubate_Date_Time,
trim(ExtubInOR) as Extubate_In_OR_Id,
trim(ReIntubate) as Reintubate_Id,
cast(cast(FinExtubDt as varchar(19)) as timestamp(0)) as Final_Extubate_Date_Time,
trim(ReOpAftrOpInAdm) as Reoperation_After_Operation_Id,
cast(cast(CreateDate as varchar(19)) as timestamp(0)) as Source_Create_Date_Time,
cast(cast(LastUpdate as varchar(19)) as timestamp(0)) as Source_Last_Update_Date_Time,
TRIM(UpdateBy) as Updated_By_3_4_Id,
trim('C') as Source_System_Code,
stg.DW_Last_Update_Date_Time as DW_Last_Update_Date_Time 
from EDWCDM_STAGING.CardioAccess_PostOpData_STG stg
inner Join EDWCDM.CA_Server
on trim(Stg.Full_Server_Nm) = trim(CA_Server.Server_Name)
left outer join 
( Select Patient_Case_SK,Source_Patient_Case_Num ,Server_Name From
 EDWCDM.CA_Patient_Case  C
 Inner Join EDWCDM.CA_Server S on
 trim(C.Server_SK) = trim(S.Server_SK)
) A
 on trim(Stg.Full_Server_NM) = trim(A.Server_Name)
 and trim(Stg.CaseNumber) = trim(A.Source_Patient_Case_Num))P)Q;