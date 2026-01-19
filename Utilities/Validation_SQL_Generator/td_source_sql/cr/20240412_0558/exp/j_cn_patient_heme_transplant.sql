SELECT 'J_CN_PATIENT_HEME_TRANSPLANT'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM (
Sel iq.* from
(Sel 
Trim(HBSource) as Hashbite_SSK
From EDWCR_Staging.stg_PatientHemeTransplant PHD 
Left Outer Join EDWCR_Base_Views.Ref_Transplant_Type RTT
On Trim(PHD.Type_Type)=Trim(RTT.Transplant_Type_Name)
Left Outer Join EDWCR_Base_Views.Ref_Status RS
On Trim(PHD.TransplantCandidacyStatus)=Trim(RS.Status_Desc)
and RS.Status_Type_Desc ='Cellular Therapy' 
) iq  
Left Outer Join EDWCR_BASE_VIEWS.CN_PATIENT_HEME_TRANSPLANT CPHT
on Trim(iq.Hashbite_SSK) = Trim(CPHT.Hashbite_SSK)
where Trim(CPHT.Hashbite_SSK) is null
) iqq