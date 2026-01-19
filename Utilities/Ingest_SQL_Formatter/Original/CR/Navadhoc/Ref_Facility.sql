Select 
  Distinct REPLACE(REPLACE(ltrim(rtrim(A.FacilityName)), NCHAR(8211), NCHAR(32)), NCHAR(8217), NCHAR(32))As facility_name,
  'N' as source_system_code, 
  'v_currtimestamp' as dw_last_update_date_time 
From 
  (
    SELECT 
      ROFacility as FacilityName 
    FROM 
      Navadhoc.dbo.PatientRadiationOncology (NOLOCK) 
    union all 
    SELECT 
      TumorReferralSource as FacilityName 
    FROM 
      Navadhoc.dbo.PatientTumor (NOLOCK) 
    UNION all 
    SELECT 
      ImageCenter as FacilityName 
    FROM 
      Navadhoc.dbo.PatientImaging (NOLOCK) 
    union all 
    SELECT 
      BiopsyFacility as FacilityName 
    FROM 
      Navadhoc.dbo.PatientBiopsy (NOLOCK) 
    union all 
    SELECT 
      MOFacility as FacilityName 
    FROM 
      Navadhoc.dbo.PatientMedicalOncology (NOLOCK) 
    union all 
    SELECT 
      EndTreatmentLocation as FacilityName 
    FROM 
      Navadhoc.dbo.PatientTumor (NOLOCK) 
    union all 
    SELECT 
      SurgeryFacility as FacilityName 
    From 
      Navadhoc.dbo.PatientSurgery (NOLOCK) 
    union all 
    SELECT 
      FacilityName as FacilityName 
    From 
      Navadhoc.dbo.PatientHemeDiseaseAssessment (NOLOCK)
  ) as A