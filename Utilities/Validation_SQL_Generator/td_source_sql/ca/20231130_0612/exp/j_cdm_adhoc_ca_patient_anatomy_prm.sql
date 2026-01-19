
select '$JOBNAME' || ',' || cast(count(*) as varchar(30)) || ',' as SOURCE_STRING FROM (
SELECT DISTINCT * FROM (
SELECT 
Null AS  Patient_Anatomy_SK  ,
A.Patient_SK,
csrv.Server_SK,
STG.PatAnatomyID AS Source_Patient_Anatomy_Id,
STG.FundDiag AS Fundamental_Diagnosis_Id,
STG.Premature AS Premature_Birth_Id,
STG.GestAgeWeeks AS Gestation_Age_Week_Num,
STG.BirthWtKg AS Birth_Weight_Kg_Amt,
STG.AntDiag AS Antenatal_Diagnosis_Id,
STG.Apgar1Min AS APGAR_Score_1_Min_Num,
STG.Apgar5Min AS APGAR_Score_5_Min_Num,
STG.FundDxText AS Fundamental_Diagnosis_Text,
STG.BirthLen AS Birth_Length_Cm_Num,
STG.BirthHCircum AS Birth_Head_Circum_Cm_Num,
CAST(CAST(STG.CreateDate AS VARCHAR(19)) AS TIMESTAMP(0)) AS Source_Create_Date_Time,
CAST(CAST(STG.LastUpdate  AS VARCHAR(19)) AS TIMESTAMP(0)) AS Source_Last_Update_Date_Time,
STG.UpdatedBy AS Updated_By_3_4_Id,
STG.BirthIVF AS Birth_IVF_Id,
STG.BornLoc AS Born_In_Out_Id,
STG.MultGest AS Multiple_Gestation_Id,
STG.DelivMode AS Delivery_Mode_Id,
STG.Gravidity AS Mother_Gravidity_Num,
STG.Parity AS Mother_Parity_Num,
'C' AS Source_System_Code,
Current_Timestamp(0) AS DW_Last_Update_Date_Time
FROM EDWCDM_STAGING.CardioAccess_PatAnatomy_STG STG
LEFT JOIN (Sel Source_Patient_Id, Server_Name, Patient_SK  From EDWCDM.CA_Patient  C
Inner Join EDWCDM.CA_Server S 
On C.Server_SK = S.Server_SK
)A
On STG.PatId = A.Source_Patient_Id
and STG.Full_Server_NM = A.Server_Name
INNER JOIN  EDWCDM.CA_SERVER csrv
ON STG.Full_Server_NM  =csrv.Server_Name 
		  
LEFT JOIN EDWCDM.CA_Patient_Anatomy CH 
ON CH.Server_SK = csrv.Server_SK
AND CH.Source_Patient_Anatomy_Id = STG.PatAnatomyID
where CH.Server_SK is null and CH.Source_Patient_Anatomy_Id is null)a)b