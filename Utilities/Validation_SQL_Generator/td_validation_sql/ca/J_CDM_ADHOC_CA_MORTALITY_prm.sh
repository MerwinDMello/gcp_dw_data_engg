export JOBNAME='J_CDM_ADHOC_CA_MORTALITY'

export AC_EXP_SQL_STATEMENT="
select '$JOBNAME' || ',' || cast(count(*) as varchar(30)) || ',' as SOURCE_STRING FROM (
SELECT  * FROM (
select 
null as Mortality_SK,
A.Patient_SK as Patient_SK,
CA_Server.Server_SK as Server_SK,
TRIM(MtId) as Source_Mortality_Id ,
cast(trim(cast(cast(MtDate as  date) as varchar(10)) ||' '|| cast(cast(MtTime as Time) as varchar(8))) as timestamp(0)) as Mortality_Date_Time,
TRIM(MtLocation) AS Mortality_Location_Id,
TRIM(Mortalty) AS Mortality_Hosp_Id,
TRIM(PrimaryCause) as PrimaryCause,
 TRIM(Cardiac) as Cardiac,
 TRIM(Renal) as Renal,
 TRIM(Infection) as Infection,
 TRIM(Valvular) as Valvular,
 TRIM(Neurologic) as Neurologic,
 TRIM(Vascular) as Vascular,
 TRIM(Pulmonary) as Pulmonary,
 TRIM(Other) as Other,
 TRIM(Gi) as Gi,
 TRIM(Prematurity) as Prematurity,
 TRIM(DeathLab) as DeathLab,
 TRIM(Autopsy) as Autopsy,
 TRIM(AutopsyDx) as AutopsyDx,
 TRIM(SuspectedCauseDeath) as SuspectedCauseDeath,
 TRIM(MtAge) as MtAge,
 TRIM(DeathCause) as DeathCause,
 CAST(CAST(CreateDate  AS CHAR(19)) AS TIMESTAMP(0))  as Source_Create_Date_Time,
CAST(CAST(lastUpdate  AS CHAR(19)) AS TIMESTAMP(0))  as Source_Last_Update_Date_Time ,
TRIM(UpdateBy) AS Updated_By_3_4_Id,
'C' AS Source_System_Code, 
Current_Timestamp(0) as  DW_Last_Update_Date_Time 
from EDWCDM_STAGING.CardioAccess_Mortality_STG stg
Inner Join EDWCDM.CA_Server
 on Stg.Full_Server_Nm = CA_Server.Server_Name
left outer join 
 ( Select Patient_SK,Source_Patient_Id ,Server_Name From
 EDWCDM.CA_Patient  C
 Inner Join EDWCDM.CA_Server S on
 C.Server_SK=S.Server_SK
   ) A
 on Stg.PatId = A.Source_Patient_Id
 and Stg.Full_Server_NM=A.Server_Name)a)b;"

export AC_ACT_SQL_STATEMENT="
select '$JOBNAME'  || ','  || cast(COUNT(*) as varchar(30)) ||',' as SOURCE_STRING FROM (
Select * from  EDWCDM.CA_Mortality)a;"




