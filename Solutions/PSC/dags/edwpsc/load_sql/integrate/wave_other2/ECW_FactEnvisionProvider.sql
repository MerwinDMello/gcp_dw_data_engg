
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactEnvisionProvider AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactEnvisionProvider AS source
ON target.EnvisionProviderKey = source.EnvisionProviderKey
WHEN MATCHED THEN
  UPDATE SET
  target.EnvisionProviderKey = source.EnvisionProviderKey,
 target.NPI = TRIM(source.NPI),
 target.LastName = TRIM(source.LastName),
 target.FirstName = TRIM(source.FirstName),
 target.MiddleName = TRIM(source.MiddleName),
 target.Suffix = TRIM(source.Suffix),
 target.Degree = TRIM(source.Degree),
 target.DOB = source.DOB,
 target.PlaceOfBirth = TRIM(source.PlaceOfBirth),
 target.Gender = TRIM(source.Gender),
 target.Phone = TRIM(source.Phone),
 target.Email = TRIM(source.Email),
 target.AddressLine1 = TRIM(source.AddressLine1),
 target.AddressLine2 = TRIM(source.AddressLine2),
 target.City = TRIM(source.City),
 target.State = TRIM(source.State),
 target.Zip = TRIM(source.Zip),
 target.Speciality = TRIM(source.Speciality),
 target.Speciality2 = TRIM(source.Speciality2),
 target.TaxonomyCode = TRIM(source.TaxonomyCode),
 target.StartDate = source.StartDate,
 target.SiteCode = TRIM(source.SiteCode),
 target.PracticeAddressLine1 = TRIM(source.PracticeAddressLine1),
 target.PracticeAddressLine2 = TRIM(source.PracticeAddressLine2),
 target.PracticeCity = TRIM(source.PracticeCity),
 target.PracticeState = TRIM(source.PracticeState),
 target.PracticeZip = TRIM(source.PracticeZip),
 target.PracticePhone = TRIM(source.PracticePhone),
 target.PracticeFax = TRIM(source.PracticeFax),
 target.LicenseState = TRIM(source.LicenseState),
 target.LicenseNumber = TRIM(source.LicenseNumber),
 target.LicenseIssuedDate = source.LicenseIssuedDate,
 target.LicenseExpirationDate = source.LicenseExpirationDate,
 target.DEALicenseState = TRIM(source.DEALicenseState),
 target.DEALicenseNumber = TRIM(source.DEALicenseNumber),
 target.DEALicenseIssuedDate = source.DEALicenseIssuedDate,
 target.DEALicenseExpirationDate = source.DEALicenseExpirationDate,
 target.CDSState = TRIM(source.CDSState),
 target.CDSNumber = TRIM(source.CDSNumber),
 target.CDSIssuedDate = source.CDSIssuedDate,
 target.CDSExpirationDate = source.CDSExpirationDate,
 target.ALCSCertification = TRIM(source.ALCSCertification),
 target.ALCSCertificationExpirationDate = source.ALCSCertificationExpirationDate,
 target.BLSCertification = TRIM(source.BLSCertification),
 target.BLSCertificationExpirationDate = source.BLSCertificationExpirationDate,
 target.PALSCertification = TRIM(source.PALSCertification),
 target.PALSCertificationExpirationDate = source.PALSCertificationExpirationDate,
 target.LiabilityInsuranceCarrier = TRIM(source.LiabilityInsuranceCarrier),
 target.MalpracticeInsPolicyNumber = TRIM(source.MalpracticeInsPolicyNumber),
 target.MalpracticeInsEffectiveDate = source.MalpracticeInsEffectiveDate,
 target.MalpracticeInsExpirationDate = source.MalpracticeInsExpirationDate,
 target.ECFMGCertification = TRIM(source.ECFMGCertification),
 target.ECFMGCertificationIssuedDate = source.ECFMGCertificationIssuedDate,
 target.ForeignMedicalGraduate = TRIM(source.ForeignMedicalGraduate),
 target.FifthPathwayGraduateName = TRIM(source.FifthPathwayGraduateName),
 target.FifthPathwayGraduateStartDate = source.FifthPathwayGraduateStartDate,
 target.FifthPathwayGraduateEndDate = source.FifthPathwayGraduateEndDate,
 target.PostHighSchoolName = TRIM(source.PostHighSchoolName),
 target.PostHighSchoolStartDate = source.PostHighSchoolStartDate,
 target.PostHighSchoolEndDate = source.PostHighSchoolEndDate,
 target.PostHighSchoolCourseMajor = TRIM(source.PostHighSchoolCourseMajor),
 target.PostHighSchoolDegree = TRIM(source.PostHighSchoolDegree),
 target.MedicalSchoolName = TRIM(source.MedicalSchoolName),
 target.MedicalSchoolStartDate = source.MedicalSchoolStartDate,
 target.MedicalSchoolEndDate = source.MedicalSchoolEndDate,
 target.MedicalSchoolCourseMajor = TRIM(source.MedicalSchoolCourseMajor),
 target.MedicalSchoolDegree = TRIM(source.MedicalSchoolDegree),
 target.InternshipName = TRIM(source.InternshipName),
 target.InternshipStartDate = source.InternshipStartDate,
 target.InternshipEndDate = source.InternshipEndDate,
 target.ResidencyName = TRIM(source.ResidencyName),
 target.ResidencyStartDate = source.ResidencyStartDate,
 target.ResidencyEndDate = source.ResidencyEndDate,
 target.FellowshipName = TRIM(source.FellowshipName),
 target.FellowshipStartDate = source.FellowshipStartDate,
 target.FellowshipEndDate = source.FellowshipEndDate,
 target.CAQHID = TRIM(source.CAQHID),
 target.CAQHUserName = TRIM(source.CAQHUserName),
 target.CAQHPassword = TRIM(source.CAQHPassword),
 target.MedicarePTAN = TRIM(source.MedicarePTAN),
 target.DelgateName = TRIM(source.DelgateName),
 target.DelgateEmail = TRIM(source.DelgateEmail),
 target.DelgatePhone = TRIM(source.DelgatePhone),
 target.BoardCertificationStatus = TRIM(source.BoardCertificationStatus),
 target.BoardCertificationInstitution = TRIM(source.BoardCertificationInstitution),
 target.BoardCertificationCertifiedDate = source.BoardCertificationCertifiedDate,
 target.BoardCertificationExpirationDate = source.BoardCertificationExpirationDate,
 target.ActiveDuty = TRIM(source.ActiveDuty),
 target.LocationSupervisingProviderName = TRIM(source.LocationSupervisingProviderName),
 target.LocationSupervisingProviderNPI = TRIM(source.LocationSupervisingProviderNPI),
 target.EducationGaps = TRIM(source.EducationGaps),
 target.RelationshipType = TRIM(source.RelationshipType),
 target.ProviderSSN = TRIM(source.ProviderSSN),
 target.ProviderUserName = TRIM(source.ProviderUserName),
 target.BusinessDaysSinceLoaded = source.BusinessDaysSinceLoaded,
 target.BusinessDaysSinceStartDate = source.BusinessDaysSinceStartDate,
 target.LoadDate = source.LoadDate,
 target.Worked = source.Worked,
 target.WorkedDate = source.WorkedDate,
 target.ArtivaWorkList = source.ArtivaWorkList,
 target.ContentWorkList = source.ContentWorkList,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.SysStartTime = source.SysStartTime,
 target.SysEndTime = source.SysEndTime,
 target.DeleteFlag = source.DeleteFlag
WHEN NOT MATCHED THEN
  INSERT (EnvisionProviderKey, NPI, LastName, FirstName, MiddleName, Suffix, Degree, DOB, PlaceOfBirth, Gender, Phone, Email, AddressLine1, AddressLine2, City, State, Zip, Speciality, Speciality2, TaxonomyCode, StartDate, SiteCode, PracticeAddressLine1, PracticeAddressLine2, PracticeCity, PracticeState, PracticeZip, PracticePhone, PracticeFax, LicenseState, LicenseNumber, LicenseIssuedDate, LicenseExpirationDate, DEALicenseState, DEALicenseNumber, DEALicenseIssuedDate, DEALicenseExpirationDate, CDSState, CDSNumber, CDSIssuedDate, CDSExpirationDate, ALCSCertification, ALCSCertificationExpirationDate, BLSCertification, BLSCertificationExpirationDate, PALSCertification, PALSCertificationExpirationDate, LiabilityInsuranceCarrier, MalpracticeInsPolicyNumber, MalpracticeInsEffectiveDate, MalpracticeInsExpirationDate, ECFMGCertification, ECFMGCertificationIssuedDate, ForeignMedicalGraduate, FifthPathwayGraduateName, FifthPathwayGraduateStartDate, FifthPathwayGraduateEndDate, PostHighSchoolName, PostHighSchoolStartDate, PostHighSchoolEndDate, PostHighSchoolCourseMajor, PostHighSchoolDegree, MedicalSchoolName, MedicalSchoolStartDate, MedicalSchoolEndDate, MedicalSchoolCourseMajor, MedicalSchoolDegree, InternshipName, InternshipStartDate, InternshipEndDate, ResidencyName, ResidencyStartDate, ResidencyEndDate, FellowshipName, FellowshipStartDate, FellowshipEndDate, CAQHID, CAQHUserName, CAQHPassword, MedicarePTAN, DelgateName, DelgateEmail, DelgatePhone, BoardCertificationStatus, BoardCertificationInstitution, BoardCertificationCertifiedDate, BoardCertificationExpirationDate, ActiveDuty, LocationSupervisingProviderName, LocationSupervisingProviderNPI, EducationGaps, RelationshipType, ProviderSSN, ProviderUserName, BusinessDaysSinceLoaded, BusinessDaysSinceStartDate, LoadDate, Worked, WorkedDate, ArtivaWorkList, ContentWorkList, SourceSystemCode, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, SysStartTime, SysEndTime, DeleteFlag)
  VALUES (source.EnvisionProviderKey, TRIM(source.NPI), TRIM(source.LastName), TRIM(source.FirstName), TRIM(source.MiddleName), TRIM(source.Suffix), TRIM(source.Degree), source.DOB, TRIM(source.PlaceOfBirth), TRIM(source.Gender), TRIM(source.Phone), TRIM(source.Email), TRIM(source.AddressLine1), TRIM(source.AddressLine2), TRIM(source.City), TRIM(source.State), TRIM(source.Zip), TRIM(source.Speciality), TRIM(source.Speciality2), TRIM(source.TaxonomyCode), source.StartDate, TRIM(source.SiteCode), TRIM(source.PracticeAddressLine1), TRIM(source.PracticeAddressLine2), TRIM(source.PracticeCity), TRIM(source.PracticeState), TRIM(source.PracticeZip), TRIM(source.PracticePhone), TRIM(source.PracticeFax), TRIM(source.LicenseState), TRIM(source.LicenseNumber), source.LicenseIssuedDate, source.LicenseExpirationDate, TRIM(source.DEALicenseState), TRIM(source.DEALicenseNumber), source.DEALicenseIssuedDate, source.DEALicenseExpirationDate, TRIM(source.CDSState), TRIM(source.CDSNumber), source.CDSIssuedDate, source.CDSExpirationDate, TRIM(source.ALCSCertification), source.ALCSCertificationExpirationDate, TRIM(source.BLSCertification), source.BLSCertificationExpirationDate, TRIM(source.PALSCertification), source.PALSCertificationExpirationDate, TRIM(source.LiabilityInsuranceCarrier), TRIM(source.MalpracticeInsPolicyNumber), source.MalpracticeInsEffectiveDate, source.MalpracticeInsExpirationDate, TRIM(source.ECFMGCertification), source.ECFMGCertificationIssuedDate, TRIM(source.ForeignMedicalGraduate), TRIM(source.FifthPathwayGraduateName), source.FifthPathwayGraduateStartDate, source.FifthPathwayGraduateEndDate, TRIM(source.PostHighSchoolName), source.PostHighSchoolStartDate, source.PostHighSchoolEndDate, TRIM(source.PostHighSchoolCourseMajor), TRIM(source.PostHighSchoolDegree), TRIM(source.MedicalSchoolName), source.MedicalSchoolStartDate, source.MedicalSchoolEndDate, TRIM(source.MedicalSchoolCourseMajor), TRIM(source.MedicalSchoolDegree), TRIM(source.InternshipName), source.InternshipStartDate, source.InternshipEndDate, TRIM(source.ResidencyName), source.ResidencyStartDate, source.ResidencyEndDate, TRIM(source.FellowshipName), source.FellowshipStartDate, source.FellowshipEndDate, TRIM(source.CAQHID), TRIM(source.CAQHUserName), TRIM(source.CAQHPassword), TRIM(source.MedicarePTAN), TRIM(source.DelgateName), TRIM(source.DelgateEmail), TRIM(source.DelgatePhone), TRIM(source.BoardCertificationStatus), TRIM(source.BoardCertificationInstitution), source.BoardCertificationCertifiedDate, source.BoardCertificationExpirationDate, TRIM(source.ActiveDuty), TRIM(source.LocationSupervisingProviderName), TRIM(source.LocationSupervisingProviderNPI), TRIM(source.EducationGaps), TRIM(source.RelationshipType), TRIM(source.ProviderSSN), TRIM(source.ProviderUserName), source.BusinessDaysSinceLoaded, source.BusinessDaysSinceStartDate, source.LoadDate, source.Worked, source.WorkedDate, source.ArtivaWorkList, source.ContentWorkList, TRIM(source.SourceSystemCode), source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.SysStartTime, source.SysEndTime, source.DeleteFlag);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT EnvisionProviderKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactEnvisionProvider
      GROUP BY EnvisionProviderKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactEnvisionProvider');
ELSE
  COMMIT TRANSACTION;
END IF;
