SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM (Sel iq.*
 FROM (Sel Trim(HBSource) AS Hashbite_SSK
 FROM EDWCR_Staging.stg_PatientHemeDiagnosis PHD
 LEFT OUTER JOIN EDWCR_Base_Views.Ref_Status RS ON Trim(PHD.DiseaseStatus)=Trim(RS.Status_Desc)
 AND Trim(RS.Status_Type_Desc) ='Disease') iq
 LEFT OUTER JOIN EDWCR_BASE_VIEWS.CN_Patient_Heme_Diagnosis CPHD ON Trim(iq.Hashbite_SSK) = Trim(CPHD.Hashbite_SSK)
 WHERE Trim(CPHD.Hashbite_SSK) IS NULL ) iqq