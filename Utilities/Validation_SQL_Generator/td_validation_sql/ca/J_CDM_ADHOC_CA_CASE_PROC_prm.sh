export JOBNAME='J_CDM_ADHOC_CA_CASE_PROC'

export AC_EXP_SQL_STATEMENT="
select '$JOBNAME' || ',' || cast(count(*) as varchar(30)) || ',' as SOURCE_STRING 
FROM (
SELECT  * FROM (
select
1 as Case_Proc_SK,
trim(A.Patient_Case_SK) as Patient_Case_SK,
TRIM(s.Server_SK) as Server_SK,
trim(stg.ProcId) as Proc_List_SK,
trim(stg.ProcedureID) as ProcedureID,
trim(stg.HospitalID) as HospitalID,
trim(stg.CaseNumber) as CaseNumber,
trim(stg.ProcedureName) as ProcedureName,
trim(stg.CPTCode) as CPTCode,
trim(stg.Price) as Price,
trim(stg.ProcID) as ProcID,
trim(stg.Modifier) as Modifier,
trim(stg.ProcCateg) as ProcCateg,
trim(stg.ProcShrtLst) as ProcShrtLst,
trim(stg.ICD_9Code) as ICD_9Code,
trim(stg.ICD_10Code) as ICD_10Code,
trim(stg.Code1) as Code1,
trim(stg.Code2) as Code2,
trim(stg.Code3) as Code3,
trim(stg.Code4) as Code4,
trim(stg.Code5) as Code5,
trim(stg.Code6) as Code6,
trim(stg.Code7) as Code7,
trim(stg.Code8) as Code8,
trim(stg.Code9) as Code9,
trim(stg.Code10) as Code10,
trim(stg.Code11) as Code11,
trim(stg.Code12) as Code12,
trim(stg.PrimProc) as PrimProc,
trim(stg.Sort) as Sort,
trim(stg.Other) as Other,
trim(stg.Recur) as Recur,
trim(stg.AristotleScore) as AristotleScore,
trim(stg.RACHSScore) as RACHSScore,
stg.CreateDate as CreateDate,
stg.UpdateDate as UpdateDate,
stg.UpdateBy as UpdateBy,
trim(stg.PSF) as PSF,
trim(stg.Server_Name) as Server_Name,
trim(stg.Full_Server_NM) as Full_Server_NM,
stg.DW_Last_Update_Date_Time
from EDWCDM_STAGING.CardioAccess_Procedures_STG stg
left outer join 
( 
  Select Patient_Case_SK,Source_Patient_Case_Num ,Server_Name
  From
  EDWCDM.CA_Patient_Case  C Inner Join EDWCDM.CA_Server S 
  on trim(C.Server_SK) = trim(S.Server_SK)
) A
on trim(Stg.Full_Server_NM) = trim(A.Server_Name)
and trim(Stg.CaseNumber) = trim(A.Source_Patient_Case_Num)
inner Join EDWCDM.CA_Server s
on trim(Stg.Full_Server_Nm) = trim(s.Server_Name)
left outer join 
( 
 Select Source_Proc_List_Id ,Server_Name 
 From EDWCDM.CA_Proc_List  C Inner Join EDWCDM.CA_Server P
 on trim(C.Server_SK) = trim(P.Server_SK)
) B
on  trim(Stg.ProcId) = trim(B.Source_Proc_List_Id)
and trim(Stg.Full_Server_NM) = trim(B.Server_Name))P)Q;"

export AC_ACT_SQL_STATEMENT="
 select '$JOBNAME'  || ','  || cast(COUNT(*) as varchar(30)) ||',' as SOURCE_STRING 
 FROM (Select * from  EDWCDM.CA_CASE_PROC)a;"

