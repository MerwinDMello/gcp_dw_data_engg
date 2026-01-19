export JOBNAME='J_CN_PHYSICIAN_ROLE_STG'
export AC_EXP_SQL_STATEMENT="Select 'J_CN_PHYSICIAN_ROLE_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING 
From
(
Select 	
MedicalSpecialistDimId	,
'BP'	as SpecialistName
From NavAdhoc.dbo.DimMedicalSpecialist	(NOLOCK)
where BiopsyPhysician=1	
Union	
Select 	
MedicalSpecialistDimId	,
'CP'		as SpecialistName
From NavAdhoc.dbo.DimMedicalSpecialist	(NOLOCK)
where ConsultPhysician=1	
Union	
Select 	
MedicalSpecialistDimId	,
'IP'	as SpecialistName	
From NavAdhoc.dbo.DimMedicalSpecialist	(NOLOCK)
where ImagingPhysician=1	
Union	
Select 	
MedicalSpecialistDimId	,
'Rsu'	as SpecialistName	
From NavAdhoc.dbo.DimMedicalSpecialist	(NOLOCK)
where ReconSurgeon=1	
Union	
Select 	
MedicalSpecialistDimId	,
'Sgn'	as SpecialistName	
From NavAdhoc.dbo.DimMedicalSpecialist	(NOLOCK)
where Surgeon=1	
Union
Select 	
MedicalSpecialistDimId	,
'MO'		as SpecialistName
From NavAdhoc.dbo.DimMedicalSpecialist	(NOLOCK)
where MedicalOncologist=1	
Union
Select 	
MedicalSpecialistDimId	,
'RO'	as SpecialistName	
From NavAdhoc.dbo.DimMedicalSpecialist	(NOLOCK)
where RadiationOncologist=1
) A"

export AC_ACT_SQL_STATEMENT="Select 'J_CN_PHYSICIAN_ROLE_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.CN_PHYSICIAN_ROLE_STG"
