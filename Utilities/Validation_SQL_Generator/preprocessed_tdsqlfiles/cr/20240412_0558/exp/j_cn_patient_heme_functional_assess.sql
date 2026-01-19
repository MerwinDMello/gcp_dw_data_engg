SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM EDWCR_Staging.CN_Patient_Heme_Functional_Assess_STG STG
LEFT JOIN EDWCR_Base_Views.Ref_Test_Type REF ON Ref.Test_Sub_Type_Desc =STG.TestType
AND Ref.Test_Type_Desc='Functional Assessment'
WHERE STG.HBSource NOT IN
 (SELECT Hashbite_SSK
 FROM EDWCR.CN_Patient_Heme_Func_Assess)