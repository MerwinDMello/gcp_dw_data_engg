export JOBNAME='J_CDM_ADHOC_CA_REFERRING_PHYSICIAN'

export AC_EXP_SQL_STATEMENT="
select '$JOBNAME' || ',' || cast(count(*) as varchar(30)) || ',' as SOURCE_STRING 
FROM (
SELECT  * FROM (
select 
1 as Referring_Physician_SK,
trim(A.Contact_Type_SK) as Contact_Type_SK,
trim(B.Contact_SK) as Contact_SK,
trim(P.Patient_Hosp_SK) as Patient_Hosp_SK,
TRIM(s.Server_SK) as Server_SK,
trim(stg.ReferringPhysID) as Source_Referring_Physician_Id,
cast(cast (stg.CreateDate as varchar(19)) as timestamp(0)) as Source_Create_Date_Time,
cast(cast (stg.LastUpdate as varchar(19)) as timestamp(0)) as Source_Last_Update_Date_Time,
TRIM(stg.UpdatedBy) as Updated_By_3_4_Id,
trim('C') as Source_System_Code,
Current_Timestamp(0) as  DW_Last_Update_Date_Time 
from EDWCDM_STAGING.CardioAccess_ReferringPhysicians_STG stg
left outer join 
( Select Contact_Type_SK,Source_Contact_Type_Id ,Server_Name 
  From EDWCDM.CA_Contact_Type  C Inner Join EDWCDM.CA_Server S 
  on TRIM(C.Server_SK) = TRIM(S.Server_SK)
) A
on TRIM(Stg.ContactTypeId) = trim(A.Source_Contact_Type_Id)
and trim(Stg.Full_Server_NM) = trim(A.Server_Name)
left outer join 
( Select Contact_SK,Source_Contact_Id ,Server_Name 
  From EDWCDM.CA_Contact C Inner Join EDWCDM.CA_Server S 
  on trim(C.Server_SK) = trim(S.Server_SK)
) B
on trim(Stg.ContactId) = trim(B.Source_Contact_Id)
and trim(Stg.Full_Server_NM) = trim(B.Server_Name)
left outer join 
( Select Patient_Hosp_SK,Source_Patient_Hosp_Id ,Server_Name 
  From EDWCDM.CA_Patient_Hosp C Inner Join EDWCDM.CA_Server S 
  on trim(C.Server_SK) = trim(S.Server_SK)
) P
on trim(Stg.HospitalizationId) = trim(P.Source_Patient_Hosp_Id)
and trim(Stg.Full_Server_NM) = trim(P.Server_Name)
inner Join EDWCDM.CA_Server s
on trim(Stg.Full_Server_Nm) = trim(s.Server_Name))P)Q;"

export AC_ACT_SQL_STATEMENT="
 select '$JOBNAME'  || ','  || cast(COUNT(*) as varchar(30)) ||',' as SOURCE_STRING 
 FROM (Select * from  EDWCDM.CA_REFERRING_PHYSICIAN)a;"

