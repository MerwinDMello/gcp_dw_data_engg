SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM (Sel iq.*
 FROM (Sel Trim(HBSource) AS Hashbite_SSK
 FROM EDWCR_Staging.stg_PatientHemeTransplant PHD
 LEFT OUTER JOIN EDWCR_Base_Views.Ref_Transplant_Type RTT ON Trim(PHD.Type_Type)=Trim(RTT.Transplant_Type_Name)
 LEFT OUTER JOIN EDWCR_Base_Views.Ref_Status RS ON Trim(PHD.TransplantCandidacyStatus)=Trim(RS.Status_Desc)
 AND RS.Status_Type_Desc ='Cellular Therapy') iq
 LEFT OUTER JOIN EDWCR_BASE_VIEWS.CN_PATIENT_HEME_TRANSPLANT CPHT ON Trim(iq.Hashbite_SSK) = Trim(CPHT.Hashbite_SSK)
 WHERE Trim(CPHT.Hashbite_SSK) IS NULL ) iqq