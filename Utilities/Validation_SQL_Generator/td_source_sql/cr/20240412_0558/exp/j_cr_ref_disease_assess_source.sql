SELECT 'J_CR_REF_DISEASE_ASSESS_SOURCE'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM (Sel 'X' as ccnt from
(
sel  distinct Source as Disease_Assess_Source_Name from EDWCR_Staging.PATIENT_HEME_DISEASE_ASSESSMENT_STG
where Source is not null
) SSC  
Left Outer Join EDWCR_BASE_VIEWS.REF_DISEASE_ASSESS_SOURCE RTT
on SSC.Disease_Assess_Source_Name = RTT.Disease_Assess_Source_Name
where RTT.Disease_Assess_Source_Name is null
) iq