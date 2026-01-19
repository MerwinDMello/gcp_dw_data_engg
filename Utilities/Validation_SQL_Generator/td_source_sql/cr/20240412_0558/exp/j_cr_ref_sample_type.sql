SELECT 'J_CR_REF_SAMPLE_TYPE'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM (Sel 'X' as ccnt from
(
sel  distinct Trim(SampleSourceType) as Sample_Type_Name from EDWCR_Staging.PATIENT_HEME_DISEASE_ASSESSMENT_STG
where Trim(SampleSourceType) is not null
) SSC  
Left Outer Join EDWCR_BASE_VIEWS.REF_SAMPLE_TYPE RTT
on Trim(SSC.Sample_Type_Name) = Trim(RTT.Sample_Type_Name)
where Trim(RTT.Sample_Type_Name) is null
) iq