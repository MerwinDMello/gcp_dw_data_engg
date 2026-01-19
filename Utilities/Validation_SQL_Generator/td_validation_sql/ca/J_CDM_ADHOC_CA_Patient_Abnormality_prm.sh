export JOBNAME='J_CDM_ADHOC_CA_Patient_Abnormality'

export AC_EXP_SQL_STATEMENT="
select '$JOBNAME' || ',' || cast(count(*) as varchar(30)) || ',' as SOURCE_STRING FROM (
SELECT DISTINCT * FROM (

SELECT 
	
      null AS Patient_Abnormality_SK,
      A.Patient_SK AS  Patient_SK,
      csrv.Server_SK AS  Server_SK,
      1 AS Abnormality_Type_Id ,
      css.SynUniqueID AS Source_Patient_Abnormality_Id ,
      css.Syndrome AS Abnormality_Id ,
      css.SyndromeOthSp AS Abnormality_Other_Text ,
      css.DemoDataVrsn AS Demographic_Data_Version_Num_Code ,
      CAST( CAST(css.CreateDate  AS DATE) AS TIMESTAMP(0)) AS Source_Create_Date_Time,
      CAST( CAST( css.LastUpdate   AS DATE) AS TIMESTAMP(0)) AS Source_Last_Update_Date_Time,
      css.UpdateBy AS Updated_By_3_4_Id ,
      css.Sort AS Source_Sort_Num ,
      'C' AS Source_System_Code,
      Current_Timestamp(0) AS DW_LAST_UPDATE_DATE_TIME
	  
FROM EDWCDM_STAGING.CardioAccess_Syndrome_STG css
	
	
LEFT JOIN (Sel Source_Patient_Id, Server_Name, Patient_Sk From EDWCDM.CA_PATIENT  C
Inner Join EDWCDM.CA_Server S 
On C.Server_SK = S.Server_SK
)A
On css.PatID = A.Source_Patient_Id
and css.Full_Server_NM = A.Server_Name


INNER JOIN  EDWCDM.CA_SERVER csrv
ON  css.Full_Server_NM  =csrv.Server_Name  
	 
LEFT JOIN EDWCDM.CA_Patient_Abnormality CH 
ON CH.Server_SK = csrv.Server_SK
AND CH.Source_Patient_Abnormality_Id = css.SynUniqueID
where CH.Server_SK is null and CH.Source_Patient_Abnormality_Id is null
	 
UNION
	 
	 
SELECT 
	
      null  AS Patient_Abnormality_SK,
      A.Patient_SK AS  Patient_SK,
      csrv.Server_SK AS  Server_SK,
      2 AS Abnormality_Type_Id ,
      ccab.ChromAbUniqueID AS Source_Patient_Abnormality_Id ,
      ccab.ChromAb AS Abnormality_Id ,
      ccab.ChromAbOthSp AS Abnormality_Other_Text ,
      ccab.DemoDataVrsn AS Demographic_Data_Version_Num_Code ,
      CAST( CAST(ccab.CreateDate  AS DATE) AS TIMESTAMP(0)) AS Source_Create_Date_Time,
      CAST( CAST( ccab.LastUpdate   AS DATE) AS TIMESTAMP(0)) AS Source_Last_Update_Date_Time,
      ccab.UpdateBy AS Updated_By_3_4_Id ,
      ccab.Sort AS Source_Sort_Num ,
      'C' AS Source_System_Code,
      Current_Timestamp(0) AS DW_LAST_UPDATE_DATE_TIME
	  
FROM EDWCDM_STAGING.CardioAccess_ChromAbnormalities_STG ccab
LEFT JOIN (Sel Source_Patient_Id, Server_Name, Patient_Sk From EDWCDM.CA_PATIENT  C
Inner Join EDWCDM.CA_Server S 
On C.Server_SK = S.Server_SK
)A
ON ccab.PatID =A.Source_Patient_Id
and ccab.Full_Server_NM = A.Server_Name

INNER JOIN  EDWCDM.CA_SERVER csrv
ON ccab.Full_Server_NM  =csrv.Server_Name  
	  
LEFT JOIN EDWCDM.CA_Patient_Abnormality CH 
ON CH.Server_SK = csrv.Server_SK
AND CH.Source_Patient_Abnormality_Id = ccab.ChromAbUniqueID 
where CH.Server_SK is null and CH.Source_Patient_Abnormality_Id is null
	 
	  
UNION
	 
	 
SELECT 
	
      null AS Patient_Abnormality_SK,
      A.Patient_SK AS  Patient_SK,
      csrv.Server_SK AS  Server_SK,
      3 AS Abnormality_Type_Id ,
      ns.NCAAUniqueID AS Source_Patient_Abnormality_Id ,
      ns.NCAA AS Abnormality_Id ,
      ns.NCAAOthSp AS Abnormality_Other_Text ,
      ns.DemoDataVrsn AS Demographic_Data_Version_Num_Code ,
      CAST( CAST(ns.CreateDate  AS DATE) AS TIMESTAMP(0)) AS Source_Create_Date_Time,
      CAST( CAST( ns.LastUpdate   AS DATE) AS TIMESTAMP(0)) AS Source_Last_Update_Date_Time,
      ns.UpdatedBy AS Updated_By_3_4_Id ,
      ns.Sort AS Source_Sort_Num ,
      'C' AS Source_System_Code,
      Current_Timestamp(0) AS DW_LAST_UPDATE_DATE_TIME
	  
FROM EDWCDM_STAGING.CardioAccess_NCAA_STG ns
LEFT JOIN (Sel Source_Patient_Id, Server_Name, Patient_Sk From EDWCDM.CA_PATIENT  C
Inner Join EDWCDM.CA_Server S 
On C.Server_SK = S.Server_SK
)A
ON ns.PatID =A.Source_Patient_Id
and ns.Full_Server_NM = A.Server_Name


INNER JOIN  EDWCDM.CA_SERVER csrv
ON ns.Full_Server_NM  =csrv.Server_Name


LEFT JOIN EDWCDM.CA_Patient_Abnormality CH 
ON CH.Server_SK = csrv.Server_SK
AND CH.Source_Patient_Abnormality_Id = ns.NCAAUniqueID
where CH.Server_SK is null and CH.Source_Patient_Abnormality_Id is null
	 
)b)c;"

export AC_ACT_SQL_STATEMENT="select 'J_CDM_ADHOC_CA_Patient_Abnormality'||','||
Coalesce(cast(count(*) as varchar(20)), 0)||',' as SOURCE_STRING 
FROM EDWCDM.CA_Patient_Abnormality 
WHERE DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCDM_AC.ETL_JOB_RUN where Job_Name = 'J_CDM_ADHOC_CA_Patient_Abnormality')
"


