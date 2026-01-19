
select '$JOBNAME' || ',' || cast(count(*) as varchar(30)) || ',' as SOURCE_STRING FROM (
SELECT DISTINCT * FROM (
SELECT DISTINCT  DiagnosisID, Full_Server_NM, 210 AS Diagnosis_Detail_Measure_Id, DiagCateg AS Diagnosis_Detail_Measure_Value_Text ,CAST(NULL AS INTEGER) AS Diagnosis_Detail_Measure_Value_Num 
FROM EDWCDM_STAGING.CardioAccess_Diagnosis_STG 
WHERE Diagnosis_Detail_Measure_Value_Text IS NOT NULL
UNION
SELECT DISTINCT  DiagnosisID, Full_Server_NM, 211 AS Diagnosis_Detail_Measure_Id, Code1 AS Diagnosis_Detail_Measure_Value_Text ,NULL AS Diagnosis_Detail_Measure_Value_Num 
FROM EDWCDM_STAGING.CardioAccess_Diagnosis_STG 
WHERE Diagnosis_Detail_Measure_Value_Text IS NOT NULL
UNION
SELECT DISTINCT  DiagnosisID, Full_Server_NM, 212 AS Diagnosis_Detail_Measure_Id, Code2 AS Diagnosis_Detail_Measure_Value_Text ,NULL AS Diagnosis_Detail_Measure_Value_Num 
FROM EDWCDM_STAGING.CardioAccess_Diagnosis_STG 
WHERE Diagnosis_Detail_Measure_Value_Text IS NOT NULL
UNION
SELECT DISTINCT  DiagnosisID, Full_Server_NM, 213 AS Diagnosis_Detail_Measure_Id, Code3 AS Diagnosis_Detail_Measure_Value_Text ,NULL AS Diagnosis_Detail_Measure_Value_Num 
FROM EDWCDM_STAGING.CardioAccess_Diagnosis_STG 
WHERE Diagnosis_Detail_Measure_Value_Text IS NOT NULL
UNION
SELECT DISTINCT  DiagnosisID, Full_Server_NM, 214 AS Diagnosis_Detail_Measure_Id, Code4 AS Diagnosis_Detail_Measure_Value_Text ,NULL AS Diagnosis_Detail_Measure_Value_Num 
FROM EDWCDM_STAGING.CardioAccess_Diagnosis_STG 
WHERE Diagnosis_Detail_Measure_Value_Text IS NOT NULL
UNION
SELECT DISTINCT  DiagnosisID, Full_Server_NM, 215 AS Diagnosis_Detail_Measure_Id, Code5 AS Diagnosis_Detail_Measure_Value_Text ,NULL AS Diagnosis_Detail_Measure_Value_Num 
FROM EDWCDM_STAGING.CardioAccess_Diagnosis_STG 
WHERE Diagnosis_Detail_Measure_Value_Text IS NOT NULL
UNION
SELECT DISTINCT  DiagnosisID, Full_Server_NM, 216 AS Diagnosis_Detail_Measure_Id, Code6 AS Diagnosis_Detail_Measure_Value_Text ,NULL AS Diagnosis_Detail_Measure_Value_Num 
FROM EDWCDM_STAGING.CardioAccess_Diagnosis_STG 
WHERE Diagnosis_Detail_Measure_Value_Text IS NOT NULL
UNION
SELECT DISTINCT  DiagnosisID, Full_Server_NM, 217 AS Diagnosis_Detail_Measure_Id, Code7 AS Diagnosis_Detail_Measure_Value_Text ,NULL AS Diagnosis_Detail_Measure_Value_Num 
FROM EDWCDM_STAGING.CardioAccess_Diagnosis_STG 
WHERE Diagnosis_Detail_Measure_Value_Text IS NOT NULL
UNION
SELECT DISTINCT  DiagnosisID, Full_Server_NM, 218 AS Diagnosis_Detail_Measure_Id, Code8 AS Diagnosis_Detail_Measure_Value_Text ,NULL AS Diagnosis_Detail_Measure_Value_Num 
FROM EDWCDM_STAGING.CardioAccess_Diagnosis_STG 
WHERE Diagnosis_Detail_Measure_Value_Text IS NOT NULL
UNION
SELECT DISTINCT  DiagnosisID, Full_Server_NM, 219 AS Diagnosis_Detail_Measure_Id, Code9 AS Diagnosis_Detail_Measure_Value_Text ,NULL AS Diagnosis_Detail_Measure_Value_Num 
FROM EDWCDM_STAGING.CardioAccess_Diagnosis_STG 
WHERE Diagnosis_Detail_Measure_Value_Text IS NOT NULL
UNION
SELECT DISTINCT  DiagnosisID, Full_Server_NM, 220 AS Diagnosis_Detail_Measure_Id, Code10 AS Diagnosis_Detail_Measure_Value_Text ,NULL AS Diagnosis_Detail_Measure_Value_Num 
FROM EDWCDM_STAGING.CardioAccess_Diagnosis_STG 
WHERE Diagnosis_Detail_Measure_Value_Text IS NOT NULL
UNION
SELECT DISTINCT  DiagnosisID, Full_Server_NM, 221 AS Diagnosis_Detail_Measure_Id, Code11 AS Diagnosis_Detail_Measure_Value_Text ,NULL AS Diagnosis_Detail_Measure_Value_Num 
FROM EDWCDM_STAGING.CardioAccess_Diagnosis_STG 
WHERE Diagnosis_Detail_Measure_Value_Text IS NOT NULL
UNION
SELECT DISTINCT  DiagnosisID, Full_Server_NM, 222 AS Diagnosis_Detail_Measure_Id, Code12 AS Diagnosis_Detail_Measure_Value_Text ,NULL AS Diagnosis_Detail_Measure_Value_Num 
FROM EDWCDM_STAGING.CardioAccess_Diagnosis_STG 
WHERE Diagnosis_Detail_Measure_Value_Text IS NOT NULL
UNION
SELECT DISTINCT  DiagnosisID, Full_Server_NM, 223 AS Diagnosis_Detail_Measure_Id, OldDiagName AS Diagnosis_Detail_Measure_Value_Text ,NULL AS Diagnosis_Detail_Measure_Value_Num 
FROM EDWCDM_STAGING.CardioAccess_Diagnosis_STG 
WHERE Diagnosis_Detail_Measure_Value_Text IS NOT NULL
UNION
SELECT DISTINCT  DiagnosisID, Full_Server_NM, 224 AS Diagnosis_Detail_Measure_Id, "Class" AS Diagnosis_Detail_Measure_Value_Text ,NULL AS Diagnosis_Detail_Measure_Value_Num 
FROM EDWCDM_STAGING.CardioAccess_Diagnosis_STG 
WHERE Diagnosis_Detail_Measure_Value_Text IS NOT NULL
UNION
SELECT DISTINCT  DiagnosisID, Full_Server_NM, 225 AS Diagnosis_Detail_Measure_Id, CAST(NULL AS VARCHAR(255))  AS Diagnosis_Detail_Measure_Value_Text ,Recur AS Diagnosis_Detail_Measure_Value_Num 
FROM EDWCDM_STAGING.CardioAccess_Diagnosis_STG 
WHERE Diagnosis_Detail_Measure_Value_Num IS NOT NULL
)a)b