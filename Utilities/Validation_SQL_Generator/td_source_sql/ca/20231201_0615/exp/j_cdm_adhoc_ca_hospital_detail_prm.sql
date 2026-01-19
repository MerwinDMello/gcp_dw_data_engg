
select '$JOBNAME' || ',' || cast(count(*) as varchar(30)) || ',' as SOURCE_STRING FROM (
SELECT DISTINCT * FROM (
SELECT CAH.Hospital_SK AS Hospital_SK
	,65 AS Hospital_Detail_Measure_Id
	,HospNumber AS Hospital_Detail_Measure_Value_Text 
	,CAST(NULL AS INTEGER)  AS Hospital_Detail_Measure_Value_Num 
FROM EDWCDM_STAGING.CardioAccess_Hospital_STG CAHS
	INNER JOIN EDWCDM.CA_SERVER CAS
	  ON
	  	CAS.Server_Name = CAHS.Full_Server_Nm 
	INNER JOIN EDWCDM.CA_Hospital CAH
		ON CAS.Server_SK = CAH.Server_SK 
		AND CAHS.HospitalID = CAH.Source_Hospital_Id
WHERE Hospital_Detail_Measure_Value_Text IS NOT NULL
UNION
SELECT CAH.Hospital_SK
	,66 AS Hospital_Detail_Measure_Id
	,ContactFName AS Hospital_Detail_Measure_Value_Text
	,CAST(NULL AS INTEGER)  AS Hospital_Detail_Measure_Value_Num 
FROM EDWCDM_STAGING.CardioAccess_Hospital_STG CAHS
	INNER JOIN EDWCDM.CA_SERVER CAS
	  ON
	  	CAS.Server_Name = CAHS.Full_Server_Nm 
	INNER JOIN EDWCDM.CA_Hospital CAH
		ON CAS.Server_SK = CAH.Server_SK 
		AND CAHS.HospitalID = CAH.Source_Hospital_Id
WHERE Hospital_Detail_Measure_Value_Text IS NOT NULL
UNION
SELECT CAH.Hospital_SK
	,67 AS Hospital_Detail_Measure_Id
	,ContactMInit AS Hospital_Detail_Measure_Value_Text
	,CAST(NULL AS INTEGER)  AS Hospital_Detail_Measure_Value_Num 
FROM EDWCDM_STAGING.CardioAccess_Hospital_STG CAHS
	INNER JOIN EDWCDM.CA_SERVER CAS
	  ON
	  	CAS.Server_Name = CAHS.Full_Server_Nm 
	INNER JOIN EDWCDM.CA_Hospital CAH
		ON CAS.Server_SK = CAH.Server_SK 
		AND CAHS.HospitalID = CAH.Source_Hospital_Id
WHERE Hospital_Detail_Measure_Value_Text IS NOT NULL
UNION
SELECT CAH.Hospital_SK
	,68 AS Hospital_Detail_Measure_Id
	,ContactLName AS Hospital_Detail_Measure_Value_Text
	,CAST(NULL AS INTEGER)  AS Hospital_Detail_Measure_Value_Num 
FROM EDWCDM_STAGING.CardioAccess_Hospital_STG CAHS
	INNER JOIN EDWCDM.CA_SERVER CAS
	  ON
	  	CAS.Server_Name = CAHS.Full_Server_Nm 
	INNER JOIN EDWCDM.CA_Hospital CAH
		ON CAS.Server_SK = CAH.Server_SK 
		AND CAHS.HospitalID = CAH.Source_Hospital_Id
WHERE Hospital_Detail_Measure_Value_Text IS NOT NULL
UNION
SELECT CAH.Hospital_SK
	,69 AS Hospital_Detail_Measure_Id
	,Email AS Hospital_Detail_Measure_Value_Text
	,CAST(NULL AS INTEGER)  AS Hospital_Detail_Measure_Value_Num 
FROM EDWCDM_STAGING.CardioAccess_Hospital_STG CAHS
	INNER JOIN EDWCDM.CA_SERVER CAS
	  ON
	  	CAS.Server_Name = CAHS.Full_Server_Nm 
	INNER JOIN EDWCDM.CA_Hospital CAH
		ON CAS.Server_SK = CAH.Server_SK 
		AND CAHS.HospitalID = CAH.Source_Hospital_Id
WHERE Hospital_Detail_Measure_Value_Text IS NOT NULL
UNION
SELECT CAH.Hospital_SK
	,70 AS Hospital_Detail_Measure_Id
	,HospPFI AS Hospital_Detail_Measure_Value_Text
	,CAST(NULL AS INTEGER)  AS Hospital_Detail_Measure_Value_Num 
FROM EDWCDM_STAGING.CardioAccess_Hospital_STG CAHS
	INNER JOIN EDWCDM.CA_SERVER CAS
	  ON
	  	CAS.Server_Name = CAHS.Full_Server_Nm 
	INNER JOIN EDWCDM.CA_Hospital CAH
		ON CAS.Server_SK = CAH.Server_SK 
		AND CAHS.HospitalID = CAH.Source_Hospital_Id
WHERE Hospital_Detail_Measure_Value_Text IS NOT NULL
UNION
SELECT CAH.Hospital_SK
	,71 AS Hospital_Detail_Measure_Id
	,ECLSCenterID AS Hospital_Detail_Measure_Value_Text
	,CAST(NULL AS INTEGER)  AS Hospital_Detail_Measure_Value_Num 
FROM EDWCDM_STAGING.CardioAccess_Hospital_STG CAHS
	INNER JOIN EDWCDM.CA_SERVER CAS
	  ON
	  	CAS.Server_Name = CAHS.Full_Server_Nm 
	INNER JOIN EDWCDM.CA_Hospital CAH
		ON CAS.Server_SK = CAH.Server_SK 
		AND CAHS.HospitalID = CAH.Source_Hospital_Id
WHERE Hospital_Detail_Measure_Value_Text IS NOT NULL
UNION
SELECT CAH.Hospital_SK
	,72 AS Hospital_Detail_Measure_Id
	,CAST(NULL AS VARCHAR(255)) AS Hospital_Detail_Measure_Value_Text
	,ParticipantId  AS Hospital_Detail_Measure_Value_Num 
FROM EDWCDM_STAGING.CardioAccess_Hospital_STG CAHS
	INNER JOIN EDWCDM.CA_SERVER CAS
	  ON
	  	CAS.Server_Name = CAHS.Full_Server_Nm 
	INNER JOIN EDWCDM.CA_Hospital CAH
		ON CAS.Server_SK = CAH.Server_SK 
		AND CAHS.HospitalID = CAH.Source_Hospital_Id
WHERE Hospital_Detail_Measure_Value_Num IS NOT NULL
UNION
SELECT CAH.Hospital_SK
	,73 AS Hospital_Detail_Measure_Id
	,EncryptionKey AS Hospital_Detail_Measure_Value_Text
	,CAST(NULL AS INTEGER)  AS Hospital_Detail_Measure_Value_Num 
FROM EDWCDM_STAGING.CardioAccess_Hospital_STG CAHS
	INNER JOIN EDWCDM.CA_SERVER CAS
	  ON
	  	CAS.Server_Name = CAHS.Full_Server_Nm 
	INNER JOIN EDWCDM.CA_Hospital CAH
		ON CAS.Server_SK = CAH.Server_SK 
		AND CAHS.HospitalID = CAH.Source_Hospital_Id
WHERE Hospital_Detail_Measure_Value_Text IS NOT NULL
UNION
SELECT CAH.Hospital_SK
	,74 AS Hospital_Detail_Measure_Id
	,Auxiliary0 AS Hospital_Detail_Measure_Value_Text
	,CAST(NULL AS INTEGER)  AS Hospital_Detail_Measure_Value_Num 
FROM EDWCDM_STAGING.CardioAccess_Hospital_STG CAHS
	INNER JOIN EDWCDM.CA_SERVER CAS
	  ON
	  	CAS.Server_Name = CAHS.Full_Server_Nm 
	INNER JOIN EDWCDM.CA_Hospital CAH
		ON CAS.Server_SK = CAH.Server_SK 
		AND CAHS.HospitalID = CAH.Source_Hospital_Id
WHERE Hospital_Detail_Measure_Value_Text IS NOT NULL
UNION
SELECT CAH.Hospital_SK
	,75 AS Hospital_Detail_Measure_Id
	,HospGUID AS Hospital_Detail_Measure_Value_Text
	,CAST(NULL AS INTEGER)  AS Hospital_Detail_Measure_Value_Num 
FROM EDWCDM_STAGING.CardioAccess_Hospital_STG CAHS
	INNER JOIN EDWCDM.CA_SERVER CAS
	  ON
	  	CAS.Server_Name = CAHS.Full_Server_Nm 
	INNER JOIN EDWCDM.CA_Hospital CAH
		ON CAS.Server_SK = CAH.Server_SK 
		AND CAHS.HospitalID = CAH.Source_Hospital_Id
WHERE Hospital_Detail_Measure_Value_Text IS NOT NULL
UNION
SELECT CAH.Hospital_SK
	,76 AS Hospital_Detail_Measure_Id
	,PC4SiteID AS Hospital_Detail_Measure_Value_Text
	,CAST(NULL AS INTEGER)  AS Hospital_Detail_Measure_Value_Num 
FROM EDWCDM_STAGING.CardioAccess_Hospital_STG CAHS
	INNER JOIN EDWCDM.CA_SERVER CAS
	  ON
	  	CAS.Server_Name = CAHS.Full_Server_Nm 
	INNER JOIN EDWCDM.CA_Hospital CAH
		ON CAS.Server_SK = CAH.Server_SK 
		AND CAHS.HospitalID = CAH.Source_Hospital_Id
WHERE Hospital_Detail_Measure_Value_Text IS NOT NULL
UNION
SELECT CAH.Hospital_SK
	,77 AS Hospital_Detail_Measure_Id
	,PC4EncryptionKey AS Hospital_Detail_Measure_Value_Text
	,CAST(NULL AS INTEGER)  AS Hospital_Detail_Measure_Value_Num 
FROM EDWCDM_STAGING.CardioAccess_Hospital_STG CAHS
	INNER JOIN EDWCDM.CA_SERVER CAS
	  ON
	  	CAS.Server_Name = CAHS.Full_Server_Nm 
	INNER JOIN EDWCDM.CA_Hospital CAH
		ON CAS.Server_SK = CAH.Server_SK 
		AND CAHS.HospitalID = CAH.Source_Hospital_Id
WHERE Hospital_Detail_Measure_Value_Text IS NOT NULL
UNION
SELECT CAH.Hospital_SK
	,78 AS Hospital_Detail_Measure_Id
	,CParticipantID AS Hospital_Detail_Measure_Value_Text
	,CAST(NULL AS INTEGER)  AS Hospital_Detail_Measure_Value_Num 
FROM EDWCDM_STAGING.CardioAccess_Hospital_STG CAHS
	INNER JOIN EDWCDM.CA_SERVER CAS
	  ON
	  	CAS.Server_Name = CAHS.Full_Server_Nm 
	INNER JOIN EDWCDM.CA_Hospital CAH
		ON CAS.Server_SK = CAH.Server_SK 
		AND CAHS.HospitalID = CAH.Source_Hospital_Id
WHERE Hospital_Detail_Measure_Value_Text IS NOT NULL
UNION
SELECT CAH.Hospital_SK
	,79 AS Hospital_Detail_Measure_Id
	,ACCHospNPI AS Hospital_Detail_Measure_Value_Text
	,CAST(NULL AS INTEGER)  AS Hospital_Detail_Measure_Value_Num 
FROM EDWCDM_STAGING.CardioAccess_Hospital_STG CAHS
	INNER JOIN EDWCDM.CA_SERVER CAS
	  ON
	  	CAS.Server_Name = CAHS.Full_Server_Nm 
	INNER JOIN EDWCDM.CA_Hospital CAH
		ON CAS.Server_SK = CAH.Server_SK 
		AND CAHS.HospitalID = CAH.Source_Hospital_Id
WHERE Hospital_Detail_Measure_Value_Text IS NOT NULL
UNION
SELECT CAH.Hospital_SK
	,80 AS Hospital_Detail_Measure_Id
	,PC4HospNPI AS Hospital_Detail_Measure_Value_Text
	,CAST(NULL AS INTEGER)  AS Hospital_Detail_Measure_Value_Num 
FROM EDWCDM_STAGING.CardioAccess_Hospital_STG CAHS
	INNER JOIN EDWCDM.CA_SERVER CAS
	  ON
	  	CAS.Server_Name = CAHS.Full_Server_Nm 
	INNER JOIN EDWCDM.CA_Hospital CAH
		ON CAS.Server_SK = CAH.Server_SK 
		AND CAHS.HospitalID = CAH.Source_Hospital_Id
WHERE Hospital_Detail_Measure_Value_Text IS NOT NULL
UNION
SELECT CAH.Hospital_SK
	,81 AS Hospital_Detail_Measure_Id
	,PhoneNumber AS Hospital_Detail_Measure_Value_Text
	,CAST(NULL AS INTEGER)  AS Hospital_Detail_Measure_Value_Num 
FROM EDWCDM_STAGING.CardioAccess_Hospital_STG CAHS
	INNER JOIN EDWCDM.CA_SERVER CAS
	  ON
	  	CAS.Server_Name = CAHS.Full_Server_Nm 
	INNER JOIN EDWCDM.CA_Hospital CAH
		ON CAS.Server_SK = CAH.Server_SK 
		AND CAHS.HospitalID = CAH.Source_Hospital_Id
WHERE Hospital_Detail_Measure_Value_Text IS NOT NULL
UNION
SELECT CAH.Hospital_SK
	,82 AS Hospital_Detail_Measure_Id
	,FaxNumber AS Hospital_Detail_Measure_Value_Text
	,CAST(NULL AS INTEGER)  AS Hospital_Detail_Measure_Value_Num 
FROM EDWCDM_STAGING.CardioAccess_Hospital_STG CAHS
	INNER JOIN EDWCDM.CA_SERVER CAS
	  ON
	  	CAS.Server_Name = CAHS.Full_Server_Nm 
	INNER JOIN EDWCDM.CA_Hospital CAH
		ON CAS.Server_SK = CAH.Server_SK 
		AND CAHS.HospitalID = CAH.Source_Hospital_Id
WHERE Hospital_Detail_Measure_Value_Text IS NOT NULL) a	
WHERE TRIM(Hospital_Detail_Measure_Value_Text) <> ''
		OR Hospital_Detail_Measure_Value_Num IS NOT NULL ) b;