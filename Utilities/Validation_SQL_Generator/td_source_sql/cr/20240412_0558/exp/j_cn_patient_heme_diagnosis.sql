SELECT 'J_CN_PATIENT_HEME_DIAGNOSIS'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM (
Sel iq.* from
(Sel 
Trim(HBSource) as Hashbite_SSK
From EDWCR_Staging.stg_PatientHemeDiagnosis PHD 
Left Outer Join EDWCR_Base_Views.Ref_Status RS
On Trim(PHD.DiseaseStatus)=Trim(RS.Status_Desc)
and Trim(RS.Status_Type_Desc) ='Disease') iq 
Left Outer Join EDWCR_BASE_VIEWS.CN_Patient_Heme_Diagnosis CPHD
on Trim(iq.Hashbite_SSK) = Trim(CPHD.Hashbite_SSK)
where Trim(CPHD.Hashbite_SSK) is null
) iqq