SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM (Sel 'X' AS ccnt
 FROM (sel DISTINCT Type_Type AS Transplant_Type_Name
 FROM EDWCR_Staging.stg_PatientHemeTransplant
 WHERE Type_Type IS NOT NULL ) SSC
 LEFT OUTER JOIN EDWCR_BASE_VIEWS.REF_TRANSPLANT_TYPE RTT ON SSC.Transplant_Type_Name = RTT.Transplant_Type_Name
 WHERE RTT.Transplant_Type_Name IS NULL ) iq