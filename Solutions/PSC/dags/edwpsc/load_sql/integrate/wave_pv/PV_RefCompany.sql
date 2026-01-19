
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_RefCompany AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_RefCompany AS source
ON target.CompanyKey = source.CompanyKey
WHEN MATCHED THEN
  UPDATE SET
  target.CompanyKey = source.CompanyKey,
 target.Practice = TRIM(source.Practice),
 target.CmpNum = source.CmpNum,
 target.CmpName = TRIM(source.CmpName),
 target.CmpContact = TRIM(source.CmpContact),
 target.AuthList = TRIM(source.AuthList),
 target.CmpAddress1 = TRIM(source.CmpAddress1),
 target.CmpAddress2 = TRIM(source.CmpAddress2),
 target.CmpCity = TRIM(source.CmpCity),
 target.CmpState = TRIM(source.CmpState),
 target.CmpZip = TRIM(source.CmpZip),
 target.CmpCountry = TRIM(source.CmpCountry),
 target.CmpPhone = TRIM(source.CmpPhone),
 target.CmpPhoneExt = TRIM(source.CmpPhoneExt),
 target.CmpFax = TRIM(source.CmpFax),
 target.Verified = TRIM(source.Verified),
 target.Status = TRIM(source.Status),
 target.GroupName = TRIM(source.GroupName),
 target.StatementFlag = TRIM(source.StatementFlag),
 target.StatementType = TRIM(source.StatementType),
 target.StatementOrder = TRIM(source.StatementOrder),
 target.StatementNotes = TRIM(source.StatementNotes),
 target.WCPricing = TRIM(source.WCPricing),
 target.EPSPricing = TRIM(source.EPSPricing),
 target.PaymentTerm = TRIM(source.PaymentTerm),
 target.PrintSSN = TRIM(source.PrintSSN),
 target.BillingNotes = TRIM(source.BillingNotes),
 target.AlertNotes = TRIM(source.AlertNotes),
 target.EPSContact = TRIM(source.EPSContact),
 target.EPSPhone = TRIM(source.EPSPhone),
 target.EPSPhoneExt = TRIM(source.EPSPhoneExt),
 target.EPSFax = TRIM(source.EPSFax),
 target.EPSEmail = TRIM(source.EPSEmail),
 target.EPSNotes = TRIM(source.EPSNotes),
 target.EPSBillToNum = source.EPSBillToNum,
 target.EPSBillToName = TRIM(source.EPSBillToName),
 target.EPSInstructions = TRIM(source.EPSInstructions),
 target.WCContact = TRIM(source.WCContact),
 target.WCPhone = TRIM(source.WCPhone),
 target.WCPhoneExt = TRIM(source.WCPhoneExt),
 target.WCFax = TRIM(source.WCFax),
 target.WCEmail = TRIM(source.WCEmail),
 target.WCNotes = TRIM(source.WCNotes),
 target.WCBillToFlag = TRIM(source.WCBillToFlag),
 target.WCBillToNum = source.WCBillToNum,
 target.WCBillToName = TRIM(source.WCBillToName),
 target.WCInstructions = TRIM(source.WCInstructions),
 target.WCUHSFlag = TRIM(source.WCUHSFlag),
 target.WCCMSFlag = TRIM(source.WCCMSFlag),
 target.TransEmail = TRIM(source.TransEmail),
 target.TransEmailPassword = TRIM(source.TransEmailPassword),
 target.TransFaxNum = TRIM(source.TransFaxNum),
 target.TransFxTo = TRIM(source.TransFxTo),
 target.TransUseEmail = source.TransUseEmail,
 target.TransUseFx = source.TransUseFx,
 target.TransUseMail = source.TransUseMail,
 target.Complete = TRIM(source.Complete),
 target.ActionDate = source.ActionDate,
 target.LastUpdUserID = TRIM(source.LastUpdUserID),
 target.LastUpdDateTime = source.LastUpdDateTime,
 target.CompanyPK = TRIM(source.CompanyPK),
 target.EmployerPortalBaseURL = TRIM(source.EmployerPortalBaseURL),
 target.AcceptICD10 = source.AcceptICD10,
 target.ICD10EffectivityDate = source.ICD10EffectivityDate,
 target.PrintEPSRemittanceCopy = source.PrintEPSRemittanceCopy,
 target.RegionKey = source.RegionKey,
 target.WcBillToInsurancePk = TRIM(source.WcBillToInsurancePk),
 target.WcBillToCompanyPk = TRIM(source.WcBillToCompanyPk),
 target.EpsBillToCompanyPk = TRIM(source.EpsBillToCompanyPk),
 target.NumberOfEmployeesRange = TRIM(source.NumberOfEmployeesRange),
 target.CompanyPKtxt = TRIM(source.CompanyPKtxt),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.DeleteFlag = source.DeleteFlag
WHEN NOT MATCHED THEN
  INSERT (CompanyKey, Practice, CmpNum, CmpName, CmpContact, AuthList, CmpAddress1, CmpAddress2, CmpCity, CmpState, CmpZip, CmpCountry, CmpPhone, CmpPhoneExt, CmpFax, Verified, Status, GroupName, StatementFlag, StatementType, StatementOrder, StatementNotes, WCPricing, EPSPricing, PaymentTerm, PrintSSN, BillingNotes, AlertNotes, EPSContact, EPSPhone, EPSPhoneExt, EPSFax, EPSEmail, EPSNotes, EPSBillToNum, EPSBillToName, EPSInstructions, WCContact, WCPhone, WCPhoneExt, WCFax, WCEmail, WCNotes, WCBillToFlag, WCBillToNum, WCBillToName, WCInstructions, WCUHSFlag, WCCMSFlag, TransEmail, TransEmailPassword, TransFaxNum, TransFxTo, TransUseEmail, TransUseFx, TransUseMail, Complete, ActionDate, LastUpdUserID, LastUpdDateTime, CompanyPK, EmployerPortalBaseURL, AcceptICD10, ICD10EffectivityDate, PrintEPSRemittanceCopy, RegionKey, WcBillToInsurancePk, WcBillToCompanyPk, EpsBillToCompanyPk, NumberOfEmployeesRange, CompanyPKtxt, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime, SourceSystemCode, DeleteFlag)
  VALUES (source.CompanyKey, TRIM(source.Practice), source.CmpNum, TRIM(source.CmpName), TRIM(source.CmpContact), TRIM(source.AuthList), TRIM(source.CmpAddress1), TRIM(source.CmpAddress2), TRIM(source.CmpCity), TRIM(source.CmpState), TRIM(source.CmpZip), TRIM(source.CmpCountry), TRIM(source.CmpPhone), TRIM(source.CmpPhoneExt), TRIM(source.CmpFax), TRIM(source.Verified), TRIM(source.Status), TRIM(source.GroupName), TRIM(source.StatementFlag), TRIM(source.StatementType), TRIM(source.StatementOrder), TRIM(source.StatementNotes), TRIM(source.WCPricing), TRIM(source.EPSPricing), TRIM(source.PaymentTerm), TRIM(source.PrintSSN), TRIM(source.BillingNotes), TRIM(source.AlertNotes), TRIM(source.EPSContact), TRIM(source.EPSPhone), TRIM(source.EPSPhoneExt), TRIM(source.EPSFax), TRIM(source.EPSEmail), TRIM(source.EPSNotes), source.EPSBillToNum, TRIM(source.EPSBillToName), TRIM(source.EPSInstructions), TRIM(source.WCContact), TRIM(source.WCPhone), TRIM(source.WCPhoneExt), TRIM(source.WCFax), TRIM(source.WCEmail), TRIM(source.WCNotes), TRIM(source.WCBillToFlag), source.WCBillToNum, TRIM(source.WCBillToName), TRIM(source.WCInstructions), TRIM(source.WCUHSFlag), TRIM(source.WCCMSFlag), TRIM(source.TransEmail), TRIM(source.TransEmailPassword), TRIM(source.TransFaxNum), TRIM(source.TransFxTo), source.TransUseEmail, source.TransUseFx, source.TransUseMail, TRIM(source.Complete), source.ActionDate, TRIM(source.LastUpdUserID), source.LastUpdDateTime, TRIM(source.CompanyPK), TRIM(source.EmployerPortalBaseURL), source.AcceptICD10, source.ICD10EffectivityDate, source.PrintEPSRemittanceCopy, source.RegionKey, TRIM(source.WcBillToInsurancePk), TRIM(source.WcBillToCompanyPk), TRIM(source.EpsBillToCompanyPk), TRIM(source.NumberOfEmployeesRange), TRIM(source.CompanyPKtxt), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), source.DeleteFlag);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT CompanyKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_RefCompany
      GROUP BY CompanyKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_RefCompany');
ELSE
  COMMIT TRANSACTION;
END IF;
