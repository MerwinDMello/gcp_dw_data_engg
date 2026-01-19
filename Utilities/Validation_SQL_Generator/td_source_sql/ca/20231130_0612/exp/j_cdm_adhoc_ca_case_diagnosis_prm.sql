
select '$JOBNAME' || ',' || cast(count(*) as varchar(30)) || ',' as SOURCE_STRING FROM (
SELECT DISTINCT * FROM (
SELECT
null AS Case_Diagnosis_SK,
A.Patient_Case_SK AS Patient_Case_SK,
csrv.Server_SK AS Server_SK,
D.Diagnosis_List_SK AS Diagnosis_List_SK,
STG.DiagnosisID  AS Source_Case_Diagnosis_Id,
CASE WHEN STG.ICD_9Code is not null 
THEN STG.ICD_9Code 
WHEN STG.CPTCode is not null 
THEN STG.CPTCode
WHEN  ICD_10Code is not null
THEN  ICD_10Code  END AS Source_Diagnosis_Code_Text,
CASE WHEN STG.ICD_9Code = Source_Diagnosis_Code_Text
THEN '9'
WHEN STG.CPTCode = Source_Diagnosis_Code_Text
THEN '5'
WHEN ICD_10Code =Source_Diagnosis_Code_Text
THEN '10' END AS Diagnosis_Type_Code,
STG.DiagShrtLst AS Diagnosis_Short_Text,
STG.PrimDiag AS Primary_Diagnosis_Id,
STG.Sort AS Source_Sort_Num,
CAST(CAST(STG.CreateDate AS VARCHAR(19)) AS TIMESTAMP(0)) AS Source_Create_Date_Time,
CAST(CAST(STG.UpdateDate  AS VARCHAR(19)) AS TIMESTAMP(0)) AS Source_Last_Update_Date_Time,
STG.UpdateBy  AS Updated_By_3_4_Id ,
'C' AS Source_System_Code,
Current_Timestamp(0) AS DW_Last_Update_Date_Time
FROM EDWCDM_STAGING.CardioAccess_Diagnosis_STG STG 
LEFT JOIN (Sel Source_Patient_Case_Num, Server_Name, Patient_Case_SK  From EDWCDM.CA_Patient_Case C
Inner Join EDWCDM.CA_Server S 
On C.Server_SK = S.Server_SK
)A
On STG.CaseNumber = A.Source_Patient_Case_Num
and STG.Full_Server_NM = A.Server_Name
INNER JOIN  EDWCDM.CA_SERVER csrv
ON STG.Full_Server_NM  =csrv.Server_Name 
LEFT JOIN (Sel Source_Diagnosis_List_Id, Server_Name,Diagnosis_List_SK   From EDWCDM.CA_Diagnosis_List C
Inner Join EDWCDM.CA_Server S 
On C.Server_SK = S.Server_SK
)D
On STG.DiagID =D.Source_Diagnosis_List_Id
and STG.Full_Server_NM =D.Server_Name
		  
LEFT JOIN EDWCDM.CA_Case_Diagnosis CH 
ON CH.Server_SK = csrv.Server_SK
AND CH.Source_Case_Diagnosis_Id = STG.DiagnosisID
where CH.Server_SK is null and CH.Source_Case_Diagnosis_Id is null)a)b