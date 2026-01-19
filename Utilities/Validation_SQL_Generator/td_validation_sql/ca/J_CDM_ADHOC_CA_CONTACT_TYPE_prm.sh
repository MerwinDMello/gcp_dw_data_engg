export JOBNAME='J_CDM_ADHOC_CA_CONTACT_TYPE'

export AC_EXP_SQL_STATEMENT="
select '$JOBNAME' || ',' || cast(count(*) as varchar(30)) || ',' as SOURCE_STRING FROM (
SELECT  * FROM (
select 
null as Contact_Type_SK
,TRIM(EDWCDM.CA_Server.Server_SK) as Server_SK
,TRIM(Source_Contact_Type_Id) as Source_Contact_Type_Id
,TRIM(Contact_Type_Name) as Contact_Type_Name
,Source_Create_Date_Time as Source_Create_Date_Time
,Source_Last_Update_Date_Time as Source_Last_Update_Date_Time
,TRIM(Updated_By_3_4_Id) as Updated_By_3_4_Id
,'C' as Source_System_Code
,Current_Timestamp(0) as  DW_Last_Update_Date_Time 
 from  EDWCDM_STAGING.CardioAccess_Contact_Type_STG Stg
 Inner Join EDWCDM.CA_Server
 on Stg.Full_Server_Nm = CA_Server.Server_Name)a)b;"

export AC_ACT_SQL_STATEMENT="
select '$JOBNAME'  || ','  || cast(COUNT(*) as varchar(30)) ||',' as SOURCE_STRING FROM (
Select * from  EDWCDM.CA_CONTACT_TYPE)a;"
