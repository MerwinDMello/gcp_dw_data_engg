
select '$JOBNAME' || ',' || cast(count(*) as varchar(30)) || ',' as SOURCE_STRING FROM (
SELECT DISTINCT * FROM (
SELECT Distinct
 null AS Patient_Mortality_Case_SK,
 A.Patient_Case_Sk AS Patient_Case_Sk,
 STG.Mt30Stat AS Mortality_30_Day_Id,
 STG.Mt30StatMeth AS Mortality_30_Day_Method_Id,
 STG.MortCase AS Mortality_Case_Id,
 csrv.Server_SK AS Server_SK,
 STG.MortCasesID AS Source_Mortality_Case_Id,
 STG.MtOpD AS Mortality_Operative_Death_Id,
 CAST(CAST(STG.CreateDate AS VARCHAR(19)) AS TIMESTAMP(0)) AS Source_Create_Date_Time,
 CAST(CAST(STG.UpdateDate AS VARCHAR(19)) AS TIMESTAMP(0)) AS Source_Last_Update_Date_Time,
 STG.UpdateBy AS Updated_By_3_4_Id,
'C' AS Source_System_Code,
Current_Timestamp(0) AS DW_Last_Update_Date_Time
FROM EDWCDM_STAGING.CardioAccess_MortCases_STG STG
LEFT JOIN (Sel Source_Patient_Case_Num, Server_Name, Patient_Case_Sk From EDWCDM.CA_Patient_Case C
Inner Join EDWCDM.CA_Server S 
On C.Server_SK = S.Server_SK
)A
On STG.CaseNumber = A.Source_Patient_Case_Num
and STG.Full_Server_NM = A.Server_Name
INNER JOIN  EDWCDM.CA_SERVER csrv
ON STG.Full_Server_NM  =csrv.Server_Name 
		  
LEFT JOIN EDWCDM.CA_Patient_Mortality_Case CH 
ON CH.Server_SK = csrv.Server_SK
AND CH.Source_Mortality_Case_Id = STG.MortCasesID
where CH.Server_SK is null and CH.Source_Mortality_Case_Id  is null)a)b