
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CCU_FactCodeQuality AS target
USING {{ params.param_psc_stage_dataset_name }}.CCU_FactCodeQuality AS source
ON target.CodeQualityKey = source.CodeQualityKey
WHEN MATCHED THEN
  UPDATE SET
  target.CodeQualityKey = source.CodeQualityKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.ServiceDate = source.ServiceDate,
 target.ClaimCreateDate = source.ClaimCreateDate,
 target.InitialCoderPreBillEditDate = source.InitialCoderPreBillEditDate,
 target.FirstInsuranceBillDate = source.FirstInsuranceBillDate,
 target.MinFirstDenialERADate = source.MinFirstDenialERADate,
 target.NumberOfInsuranceBills = source.NumberOfInsuranceBills,
 target.CoderPreBillEditCount = source.CoderPreBillEditCount,
 target.PSSCACTGRP_Initial_Combined = TRIM(source.PSSCACTGRP_Initial_Combined),
 target.PSSCCAT_Initial_Combined = TRIM(source.PSSCCAT_Initial_Combined),
 target.PSSCSUBCATDESC_Initial_Combined = TRIM(source.PSSCSUBCATDESC_Initial_Combined),
 target.Payer1IplanName = TRIM(source.Payer1IplanName),
 target.QUALITYCATEGORY = TRIM(source.QUALITYCATEGORY),
 target.TotalChargesAmt = source.TotalChargesAmt,
 target.PaymentAmt = source.PaymentAmt,
 target.ContractualAdjAmt = source.ContractualAdjAmt,
 target.FinancialAdjAmt = source.FinancialAdjAmt,
 target.FinancialAdjCategory_Combined = TRIM(source.FinancialAdjCategory_Combined),
 target.FinancialAdjName_Combined = TRIM(source.FinancialAdjName_Combined),
 target.FirstDenialCategories = TRIM(source.FirstDenialCategories),
 target.FirstDenialCARCCodes = TRIM(source.FirstDenialCARCCodes),
 target.FirstDenialRARCCodes = TRIM(source.FirstDenialRARCCodes),
 target.FirstDenialPayerName = TRIM(source.FirstDenialPayerName),
 target.ClaimCreatedBy34Id = TRIM(source.ClaimCreatedBy34Id),
 target.ClaimCreatedByUser = TRIM(source.ClaimCreatedByUser),
 target.ClaimCreatedByDept = TRIM(source.ClaimCreatedByDept),
 target.ClaimCreatedByCompany = TRIM(source.ClaimCreatedByCompany),
 target.ClaimCount = source.ClaimCount,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.LastChangedBy = source.LastChangedBy,
 target.ChangedDate = source.ChangedDate
WHEN NOT MATCHED THEN
  INSERT (CodeQualityKey, ClaimKey, ClaimNumber, RegionKey, Coid, ServiceDate, ClaimCreateDate, InitialCoderPreBillEditDate, FirstInsuranceBillDate, MinFirstDenialERADate, NumberOfInsuranceBills, CoderPreBillEditCount, PSSCACTGRP_Initial_Combined, PSSCCAT_Initial_Combined, PSSCSUBCATDESC_Initial_Combined, Payer1IplanName, QUALITYCATEGORY, TotalChargesAmt, PaymentAmt, ContractualAdjAmt, FinancialAdjAmt, FinancialAdjCategory_Combined, FinancialAdjName_Combined, FirstDenialCategories, FirstDenialCARCCodes, FirstDenialRARCCodes, FirstDenialPayerName, ClaimCreatedBy34Id, ClaimCreatedByUser, ClaimCreatedByDept, ClaimCreatedByCompany, ClaimCount, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, LastChangedBy, ChangedDate)
  VALUES (source.CodeQualityKey, source.ClaimKey, source.ClaimNumber, source.RegionKey, TRIM(source.Coid), source.ServiceDate, source.ClaimCreateDate, source.InitialCoderPreBillEditDate, source.FirstInsuranceBillDate, source.MinFirstDenialERADate, source.NumberOfInsuranceBills, source.CoderPreBillEditCount, TRIM(source.PSSCACTGRP_Initial_Combined), TRIM(source.PSSCCAT_Initial_Combined), TRIM(source.PSSCSUBCATDESC_Initial_Combined), TRIM(source.Payer1IplanName), TRIM(source.QUALITYCATEGORY), source.TotalChargesAmt, source.PaymentAmt, source.ContractualAdjAmt, source.FinancialAdjAmt, TRIM(source.FinancialAdjCategory_Combined), TRIM(source.FinancialAdjName_Combined), TRIM(source.FirstDenialCategories), TRIM(source.FirstDenialCARCCodes), TRIM(source.FirstDenialRARCCodes), TRIM(source.FirstDenialPayerName), TRIM(source.ClaimCreatedBy34Id), TRIM(source.ClaimCreatedByUser), TRIM(source.ClaimCreatedByDept), TRIM(source.ClaimCreatedByCompany), source.ClaimCount, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.LastChangedBy, source.ChangedDate);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT CodeQualityKey
      FROM {{ params.param_psc_core_dataset_name }}.CCU_FactCodeQuality
      GROUP BY CodeQualityKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CCU_FactCodeQuality');
ELSE
  COMMIT TRANSACTION;
END IF;
