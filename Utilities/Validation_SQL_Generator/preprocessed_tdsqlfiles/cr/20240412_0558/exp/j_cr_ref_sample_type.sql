SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM (Sel 'X' AS ccnt
 FROM (sel DISTINCT Trim(SampleSourceType) AS Sample_Type_Name
 FROM EDWCR_Staging.PATIENT_HEME_DISEASE_ASSESSMENT_STG
 WHERE Trim(SampleSourceType) IS NOT NULL ) SSC
 LEFT OUTER JOIN EDWCR_BASE_VIEWS.REF_SAMPLE_TYPE RTT ON Trim(SSC.Sample_Type_Name) = Trim(RTT.Sample_Type_Name)
 WHERE Trim(RTT.Sample_Type_Name) IS NULL ) iq