
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgPatInfo AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgPatInfo AS source
ON target.PatInfoPK = source.PatInfoPK AND target.RegionKey = source.RegionKey
WHEN MATCHED THEN
  UPDATE SET
  target.Practice = TRIM(source.Practice),
 target.Pat_Num = source.Pat_Num,
 target.Last_Name = TRIM(source.Last_Name),
 target.First_Name = TRIM(source.First_Name),
 target.Middle_Name = TRIM(source.Middle_Name),
 target.SSN = TRIM(source.SSN),
 target.Marital_Status = TRIM(source.Marital_Status),
 target.Pat_Status = TRIM(source.Pat_Status),
 target.Birthday = TRIM(source.Birthday),
 target.Sex = TRIM(source.Sex),
 target.Address1 = TRIM(source.Address1),
 target.Address2 = TRIM(source.Address2),
 target.City = TRIM(source.City),
 target.State = TRIM(source.State),
 target.Zip = TRIM(source.Zip),
 target.Country = TRIM(source.Country),
 target.Pat_Phone = TRIM(source.Pat_Phone),
 target.Cell_Phone = TRIM(source.Cell_Phone),
 target.Email = TRIM(source.Email),
 target.Emp_Num = source.Emp_Num,
 target.Emp_Name = TRIM(source.Emp_Name),
 target.Emp_Status = TRIM(source.Emp_Status),
 target.Emp_Phone = TRIM(source.Emp_Phone),
 target.Emp_Phone_Ext = TRIM(source.Emp_Phone_Ext),
 target.Emp_Notes = TRIM(source.Emp_Notes),
 target.PCP_Phy_Note = TRIM(source.PCP_Phy_Note),
 target.Ref_Phy_Note = TRIM(source.Ref_Phy_Note),
 target.Ref_Hsp_Note = TRIM(source.Ref_Hsp_Note),
 target.Statement_Date = source.Statement_Date,
 target.Statement_Flag = TRIM(source.Statement_Flag),
 target.Valid_Flag = TRIM(source.Valid_Flag),
 target.Complete_Flag = TRIM(source.Complete_Flag),
 target.Pymt_Plan_Date = source.Pymt_Plan_Date,
 target.Last_Svc_Date = source.Last_Svc_Date,
 target.HIPAA_Date = source.HIPAA_Date,
 target.Clinic = TRIM(source.Clinic),
 target.Acpt_Date = source.Acpt_Date,
 target.Legacy_Notes = TRIM(source.Legacy_Notes),
 target.Hear_From = TRIM(source.Hear_From),
 target.Crt_UserNum = source.Crt_UserNum,
 target.Crt_UserID = TRIM(source.Crt_UserID),
 target.Crt_DateTime = source.Crt_DateTime,
 target.Last_Upd_UserNum = source.Last_Upd_UserNum,
 target.Last_Upd_UserID = TRIM(source.Last_Upd_UserID),
 target.Last_Upd_DateTime = source.Last_Upd_DateTime,
 target.emailPromotional = source.emailPromotional,
 target.Batch_Ltr_Date = source.Batch_Ltr_Date,
 target.Batch_Ltr_SubKey = TRIM(source.Batch_Ltr_SubKey),
 target.Batch_Ltr_Printed = TRIM(source.Batch_Ltr_Printed),
 target.Race = TRIM(source.Race),
 target.Poverty = TRIM(source.Poverty),
 target.TRANSLATION = TRIM(source.TRANSLATION),
 target.PatInfoPK = TRIM(source.PatInfoPK),
 target.PatInfoPK_txt = TRIM(source.PatInfoPK_txt),
 target.PrimaryPhysicianPK = TRIM(source.PrimaryPhysicianPK),
 target.Races = source.Races,
 target.Ethnicities = source.Ethnicities,
 target.LANGUAGE = source.LANGUAGE,
 target.CommunicationMethod = source.CommunicationMethod,
 target.WantsPortalAccess = source.WantsPortalAccess,
 target.PasswordSalt = TRIM(source.PasswordSalt),
 target.PasswordHash = TRIM(source.PasswordHash),
 target.SmokingStatus = source.SmokingStatus,
 target.PrimaryPhysicianOptOut = source.PrimaryPhysicianOptOut,
 target.PublicityCode = TRIM(source.PublicityCode),
 target.PublicityCodeEffectiveDate = source.PublicityCodeEffectiveDate,
 target.ProtectionIndicator = TRIM(source.ProtectionIndicator),
 target.ProtectionIndicatorDate = source.ProtectionIndicatorDate,
 target.ImmunizationRegistryStatus = TRIM(source.ImmunizationRegistryStatus),
 target.ImmunRegistryStatusEffectiveDate = source.ImmunRegistryStatusEffectiveDate,
 target.SmokingStatusCode = source.SmokingStatusCode,
 target.PublicityCodeContact = TRIM(source.PublicityCodeContact),
 target.ProtectionIndicatorContact = TRIM(source.ProtectionIndicatorContact),
 target.VFCCode = TRIM(source.VFCCode),
 target.PreferredReminderCommunicationMethod = source.PreferredReminderCommunicationMethod,
 target.PortalDataLastSent = source.PortalDataLastSent,
 target.VoicemailAuth = source.VoicemailAuth,
 target.VoicemailAuthDate = source.VoicemailAuthDate,
 target.BirthDate = source.BirthDate,
 target.County_Parish = TRIM(source.County_Parish),
 target.PreferredName = TRIM(source.PreferredName),
 target.AllowVM_Home = source.AllowVM_Home,
 target.AllowVM_Cell = source.AllowVM_Cell,
 target.UsesExchangePatientInfo = source.UsesExchangePatientInfo,
 target.LastLogDetailPK = TRIM(source.LastLogDetailPK),
 target.WantsInHouseDispensing = source.WantsInHouseDispensing,
 target.RegionKey = source.RegionKey,
 target.Races_Description = TRIM(source.Races_Description),
 target.Ethniciies_Description = TRIM(source.Ethniciies_Description),
 target.Language_Description = TRIM(source.Language_Description),
 target.sysstarttime = source.sysstarttime,
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourcePhysicalDeleteFlag = source.SourcePhysicalDeleteFlag
WHEN NOT MATCHED THEN
  INSERT (Practice, Pat_Num, Last_Name, First_Name, Middle_Name, SSN, Marital_Status, Pat_Status, Birthday, Sex, Address1, Address2, City, State, Zip, Country, Pat_Phone, Cell_Phone, Email, Emp_Num, Emp_Name, Emp_Status, Emp_Phone, Emp_Phone_Ext, Emp_Notes, PCP_Phy_Note, Ref_Phy_Note, Ref_Hsp_Note, Statement_Date, Statement_Flag, Valid_Flag, Complete_Flag, Pymt_Plan_Date, Last_Svc_Date, HIPAA_Date, Clinic, Acpt_Date, Legacy_Notes, Hear_From, Crt_UserNum, Crt_UserID, Crt_DateTime, Last_Upd_UserNum, Last_Upd_UserID, Last_Upd_DateTime, emailPromotional, Batch_Ltr_Date, Batch_Ltr_SubKey, Batch_Ltr_Printed, Race, Poverty, TRANSLATION, PatInfoPK, PatInfoPK_txt, PrimaryPhysicianPK, Races, Ethnicities, LANGUAGE, CommunicationMethod, WantsPortalAccess, PasswordSalt, PasswordHash, SmokingStatus, PrimaryPhysicianOptOut, PublicityCode, PublicityCodeEffectiveDate, ProtectionIndicator, ProtectionIndicatorDate, ImmunizationRegistryStatus, ImmunRegistryStatusEffectiveDate, SmokingStatusCode, PublicityCodeContact, ProtectionIndicatorContact, VFCCode, PreferredReminderCommunicationMethod, PortalDataLastSent, VoicemailAuth, VoicemailAuthDate, BirthDate, County_Parish, PreferredName, AllowVM_Home, AllowVM_Cell, UsesExchangePatientInfo, LastLogDetailPK, WantsInHouseDispensing, RegionKey, Races_Description, Ethniciies_Description, Language_Description, sysstarttime, InsertedDTM, ModifiedDTM, DWLastUpdateDateTime, SourcePhysicalDeleteFlag)
  VALUES (TRIM(source.Practice), source.Pat_Num, TRIM(source.Last_Name), TRIM(source.First_Name), TRIM(source.Middle_Name), TRIM(source.SSN), TRIM(source.Marital_Status), TRIM(source.Pat_Status), TRIM(source.Birthday), TRIM(source.Sex), TRIM(source.Address1), TRIM(source.Address2), TRIM(source.City), TRIM(source.State), TRIM(source.Zip), TRIM(source.Country), TRIM(source.Pat_Phone), TRIM(source.Cell_Phone), TRIM(source.Email), source.Emp_Num, TRIM(source.Emp_Name), TRIM(source.Emp_Status), TRIM(source.Emp_Phone), TRIM(source.Emp_Phone_Ext), TRIM(source.Emp_Notes), TRIM(source.PCP_Phy_Note), TRIM(source.Ref_Phy_Note), TRIM(source.Ref_Hsp_Note), source.Statement_Date, TRIM(source.Statement_Flag), TRIM(source.Valid_Flag), TRIM(source.Complete_Flag), source.Pymt_Plan_Date, source.Last_Svc_Date, source.HIPAA_Date, TRIM(source.Clinic), source.Acpt_Date, TRIM(source.Legacy_Notes), TRIM(source.Hear_From), source.Crt_UserNum, TRIM(source.Crt_UserID), source.Crt_DateTime, source.Last_Upd_UserNum, TRIM(source.Last_Upd_UserID), source.Last_Upd_DateTime, source.emailPromotional, source.Batch_Ltr_Date, TRIM(source.Batch_Ltr_SubKey), TRIM(source.Batch_Ltr_Printed), TRIM(source.Race), TRIM(source.Poverty), TRIM(source.TRANSLATION), TRIM(source.PatInfoPK), TRIM(source.PatInfoPK_txt), TRIM(source.PrimaryPhysicianPK), source.Races, source.Ethnicities, source.LANGUAGE, source.CommunicationMethod, source.WantsPortalAccess, TRIM(source.PasswordSalt), TRIM(source.PasswordHash), source.SmokingStatus, source.PrimaryPhysicianOptOut, TRIM(source.PublicityCode), source.PublicityCodeEffectiveDate, TRIM(source.ProtectionIndicator), source.ProtectionIndicatorDate, TRIM(source.ImmunizationRegistryStatus), source.ImmunRegistryStatusEffectiveDate, source.SmokingStatusCode, TRIM(source.PublicityCodeContact), TRIM(source.ProtectionIndicatorContact), TRIM(source.VFCCode), source.PreferredReminderCommunicationMethod, source.PortalDataLastSent, source.VoicemailAuth, source.VoicemailAuthDate, source.BirthDate, TRIM(source.County_Parish), TRIM(source.PreferredName), source.AllowVM_Home, source.AllowVM_Cell, source.UsesExchangePatientInfo, TRIM(source.LastLogDetailPK), source.WantsInHouseDispensing, source.RegionKey, TRIM(source.Races_Description), TRIM(source.Ethniciies_Description), TRIM(source.Language_Description), source.sysstarttime, source.InsertedDTM, source.ModifiedDTM, source.DWLastUpdateDateTime, source.SourcePhysicalDeleteFlag);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PatInfoPK, RegionKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgPatInfo
      GROUP BY PatInfoPK, RegionKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgPatInfo');
ELSE
  COMMIT TRANSACTION;
END IF;
