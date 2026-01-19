export JOBNAME='J_CN_PATIENT_CONTACT_DETAIL_STG'

export AC_EXP_SQL_STATEMENT="Select 'J_CN_PATIENT_CONTACT_DETAIL_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING from (
Select
PatientComunicationFactID as CN_Patient_Contact_SID
,CASE 
	WHEN ltrim(rtrim(MeasureValue)) = 'AppointmentTopics' THEN 26 
	WHEN ltrim(rtrim(MeasureValue)) = 'EducationFocus' THEN 27
    WHEN ltrim(rtrim(MeasureValue)) = 'OtherEducationFocus' THEN 28
	WHEN ltrim(rtrim(MeasureValue)) = 'EducationSource' THEN 29
	WHEN ltrim(rtrim(MeasureValue)) = 'OtherEducationSource' THEN 30
	WHEN ltrim(rtrim(MeasureValue)) = 'FollowupType' THEN 31
	WHEN ltrim(rtrim(MeasureValue)) = 'OtherFollowupType' THEN 32
	WHEN ltrim(rtrim(MeasureValue)) = 'PrimaryConcernTypes' THEN 33
	WHEN ltrim(rtrim(MeasureValue)) = 'OtherConcern' THEN 34
	WHEN ltrim(rtrim(MeasureValue)) = 'Score' THEN 35
	WHEN ltrim(rtrim(MeasureValue)) = 'ScorePct' THEN 36
	WHEN ltrim(rtrim(MeasureValue)) = 'DistressAssessmentDate' THEN 37
    WHEN ltrim(rtrim(MeasureValue)) = 'NextStep' THEN 38
	WHEN ltrim(rtrim(MeasureValue)) = 'OutCome' THEN 39
	WHEN ltrim(rtrim(MeasureValue)) = 'ContactInitiatedBy' THEN 40
	WHEN ltrim(rtrim(MeasureValue)) = 'ReferralMade' THEN 41
	WHEN ltrim(rtrim(MeasureValue)) = 'ReferralType' THEN 42
	WHEN ltrim(rtrim(MeasureValue)) = 'TraditionalReferral' THEN 43
	WHEN ltrim(rtrim(MeasureValue)) = 'NonTraditionalReferral' THEN 44
	WHEN ltrim(rtrim(MeasureValue)) = 'CommunityResource' THEN 45
	WHEN ltrim(rtrim(MeasureValue)) = 'ReferralDate' THEN 46
	WHEN ltrim(rtrim(MeasureValue)) = 'ReferralFacility' THEN 47
	WHEN ltrim(rtrim(MeasureValue)) = 'Reason' THEN 48
	WHEN ltrim(rtrim(MeasureValue)) = 'OtherAppointmentTopics' THEN 49
END as Contact_Detail_Measure_Type_Id
,Cast(Result as Varchar(255)) as Contact_Detail_Measure_Value_Text
,concat('0x',CONVERT(varchar(50),HBSource,2)) as HBSource
From PatientCommunication
Cross Apply 
(
Values 
('AppointmentTopics',AppointmentTopics),
   ('EducationFocus',EducationFocus),
   ('OtherEducationFocus',OtherEducationFocus),
   ('EducationSource',EducationSource),
   ('OtherEducationSource',OtherEducationSource),
   ('FollowupType',FollowupType),
   ('OtherFollowupType',OtherFollowupType),
   ('PrimaryConcernTypes',PrimaryConcernTypes),
   ('OtherConcern',OtherConcern),
   ('Score',Score),
   ('ScorePct',Cast(ScorePct as Varchar(255))),
   ('DistressAssessmentDate',Cast(DistressAssessmentDate as Varchar(255))),
   ('NextStep',NextStep),
   ('OutCome',OutCome),
   ('ContactInitiatedBy',ContactInitiatedBy),
   ('ReferralMade',ReferralMade),
   ('ReferralType',ReferralType),
   ('TraditionalReferral',TraditionalReferral),
   ('NonTraditionalReferral',NonTraditionalReferral),
   ('CommunityResource',CommunityResource),
   ('ReferralDate',Cast(ReferralDate as Varchar(255))),
   ('ReferralFacility',ReferralFacility),
   ('Reason',Reason),
   ('OtherAppointmentTopics',OtherAppointmentTopics)
) c (MeasureValue,Result)
Where Result IS NOT NULL AND ltrim(rtrim(Result)) != ''

)abc"

export AC_ACT_SQL_STATEMENT="Select 'J_CN_PATIENT_CONTACT_DETAIL_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.CN_Patient_Contact_Detail_STG;"
