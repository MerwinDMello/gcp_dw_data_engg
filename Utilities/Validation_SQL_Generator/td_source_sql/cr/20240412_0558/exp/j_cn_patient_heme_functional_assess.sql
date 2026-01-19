SELECT 'J_CN_PATIENT_HEME_FUNCTIONAL_ASSESS'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM 
EDWCR_Staging.CN_Patient_Heme_Functional_Assess_STG STG
LEFT JOIN EDWCR_Base_Views.Ref_Test_Type Ref 
on  Ref.Test_Sub_Type_Desc =STG.TestType 
and Ref.Test_Type_Desc='Functional Assessment'
where STG.HBSource NOT IN (
SELECT 
Hashbite_SSK
from EDWCR.CN_Patient_Heme_Func_Assess)