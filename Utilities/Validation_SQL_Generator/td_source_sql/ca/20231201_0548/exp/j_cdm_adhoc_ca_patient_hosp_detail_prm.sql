
select '$JOBNAME' || ',' || cast(count(*) as varchar(30)) || ',' as SOURCE_STRING FROM (
SELECT DISTINCT * FROM (
SELECT DISTINCT
 CAPH.Patient_Hosp_SK
 , 83 AS Patient_Hosp_Detail_Measure_Id
 , PC4Hospdatavrsn AS Patient_Hosp_Detail_Measure_Value_Text 
 , CAST(NULL AS INTEGER) AS Patient_Hosp_Detail_Measure_Value_Num 
FROM EDWCDM_STAGING.CardioAccess_Hospitalization_STG CAHS
	INNER JOIN EDWCDM.CA_SERVER CAS
		ON CAHS.Full_Server_Nm = CAS.Server_Name
	INNER JOIN EDWCDM.CA_Patient_Hosp CAPH
		ON CAHS.HospitalizationID = CAPH.Source_Patient_Hosp_Id
			AND CAS.Server_SK = CAPH.Server_SK
	WHERE Patient_Hosp_Detail_Measure_Value_Text IS NOT NULL
UNION
SELECT DISTINCT
 CAPH.Patient_Hosp_SK
 , 84 AS Patient_Hosp_Detail_Measure_Id
 , Impactdatavrsn AS Patient_Hosp_Detail_Measure_Value_Text 
 , CAST(NULL AS INTEGER)  AS Patient_Hosp_Detail_Measure_Value_Num 
FROM EDWCDM_STAGING.CardioAccess_Hospitalization_STG CAHS
	INNER JOIN EDWCDM.CA_SERVER CAS
		ON CAHS.Full_Server_Nm = CAS.Server_Name
	INNER JOIN EDWCDM.CA_Patient_Hosp CAPH
		ON CAHS.HospitalizationID = CAPH.Source_Patient_Hosp_Id
			AND CAS.Server_SK = CAPH.Server_SK
	WHERE Patient_Hosp_Detail_Measure_Value_Text IS NOT NULL
) a ) b