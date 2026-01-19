SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM (Sel iq.*
 FROM (Sel Trim(HBSource) AS Hashbite_SSK
 FROM EDWCR_Staging.PATIENT_HEME_DISEASE_ASSESSMENT_STG PHD
 LEFT OUTER JOIN EDWCR_Base_Views.Ref_Test_Type RT ON Trim(PHD.TestType)=Trim(RT.Test_Sub_Type_Desc)
 AND Trim(RT.Test_Type_Desc) ='Disease Assessment'
 LEFT OUTER JOIN EDWCR_Base_Views.Ref_Sample_Type RST ON Trim(PHD.SampleSourceType) = Trim(RST.Sample_Type_Name)
 LEFT OUTER JOIN EDWCR_Base_Views.Ref_Disease_Assess_Source RDA ON Trim(PHD.Source) = Trim(RDA.Disease_Assess_Source_Name)
 LEFT OUTER JOIN EDWCR_Base_Views.Ref_Facility RF ON Trim(PHD.FacilityName) = Trim(RF.Facility_Name)
 LEFT OUTER JOIN EDWCR_Base_Views.Ref_Status RS ON Trim(PHD.DiseaseStatus)=Trim(RS.Status_Desc)
 AND Trim(RS.Status_Type_Desc) ='Disease'
 LEFT OUTER JOIN EDWCR_Base_Views.Ref_Status RSS ON Trim(PHD.TreatementStatus)=Trim(RSS.Status_Desc)
 AND Trim(RSS.Status_Type_Desc) ='Treatment') iq
 LEFT OUTER JOIN EDWCR_BASE_VIEWS.CN_PATIENT_HEME_DISEASE_ASSESS CPHD ON Trim(iq.Hashbite_SSK) = Trim(CPHD.Hashbite_SSK)
 WHERE Trim(CPHD.Hashbite_SSK) IS NULL ) iqq