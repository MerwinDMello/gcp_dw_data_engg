export JOBNAME='J_CDM_ADHOC_CA_Diagnosis_List'

export AC_EXP_SQL_STATEMENT="
select '$JOBNAME' || ',' || cast(count(*) as varchar(30)) || ',' as SOURCE_STRING FROM (
SELECT DISTINCT * FROM (
SELECT 
Null AS Diagnosis_List_SK,
rcdc.Diagnosis_Category_Id AS Diagnosis_Category_Id,
ccs.Server_SK AS Server_SK,
STG.ID AS Source_Diagnosis_List_Id,
STG.Diagnosis AS Diagnosis_Name,
STG.ICD_9_Code AS Source_Diagnosis_Code_Text,
9 AS Diagnosis_Type_Code,
STG.WrkGrpCode AS Work_Group_Code,
STG.DBType AS Db_Type_Text,
CASE WHEN STG.Inactive= 1 
THEN 'N' 
WHEN STG.Inactive= 2 
THEN 'Y'  END AS Active_Ind,
'C' AS Source_System_Code,
Current_Timestamp(0) AS DW_Last_Update_Date_Time

FROM EDWCDM_STAGING.CardioAccess_DiagnosisList_STG STG
LEFT JOIN  EDWCDM.Ref_CA_Diagnosis_Category rcdc
ON Coalesce(STG.DxMCategory, '') = Coalesce(rcdc.Diagnosis_Category_Name, '')
AND Coalesce(STG.DxSCategory, '') = Coalesce(rcdc.Diagnosis_Sub_Category_Name, '')

INNER JOIN EDWCDM.CA_Server ccs
ON STG.Full_Server_NM = ccs.Server_Name
		  
LEFT JOIN EDWCDM.CA_Diagnosis_List CH 
ON CH.Server_SK = ccs.Server_SK
AND CH.Source_Diagnosis_List_Id = STG.ID
where CH.Server_SK is null and CH.Source_Diagnosis_List_Id is null)a)b;"

export AC_ACT_SQL_STATEMENT="select 'J_CDM_ADHOC_CA_Diagnosis_List'||','||
Coalesce(cast(count(*) as varchar(20)), 0)||',' as SOURCE_STRING 
FROM EDWCDM.CA_Diagnosis_List
WHERE DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCDM_AC.ETL_JOB_RUN where Job_Name = 'J_CDM_ADHOC_CA_Diagnosis_List')
"


