
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_FactSnapShotClaimsOnHold AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_FactSnapShotClaimsOnHold AS source
ON target.SnapShotDate = source.SnapShotDate AND target.ClaimKey = source.ClaimKey
WHEN MATCHED THEN
  UPDATE SET
  target.MonthID = source.MonthID,
 target.SnapShotDate = source.SnapShotDate,
 target.LoadDateKey = source.LoadDateKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.ClaimBalance = source.ClaimBalance,
 target.PracticeName = TRIM(source.PracticeName),
 target.RegionKey = source.RegionKey,
 target.ArtivaRuleID = source.ArtivaRuleID,
 target.ArtivaRuleDesc = TRIM(source.ArtivaRuleDesc),
 target.ArtivaRuleOwner = TRIM(source.ArtivaRuleOwner),
 target.ArtivaRuleCategoryID = source.ArtivaRuleCategoryID,
 target.ArtivaHoldCodeID = TRIM(source.ArtivaHoldCodeID),
 target.ArtivaHoldCodeDescription = TRIM(source.ArtivaHoldCodeDescription),
 target.ArtivaHoldCodeType = TRIM(source.ArtivaHoldCodeType),
 target.ArtivaPpiIDPE = source.ArtivaPpiIDPE,
 target.ArtivaPpiTypePE = TRIM(source.ArtivaPpiTypePE),
 target.ArtivaPpiIDEDI = source.ArtivaPpiIDEDI,
 target.ArtivaPpiTypeEDI = TRIM(source.ArtivaPpiTypeEDI),
 target.ArtivaPpiPhase = TRIM(source.ArtivaPpiPhase),
 target.ArtivaRuleCreateDate = source.ArtivaRuleCreateDate,
 target.ArtivaRuleActiveFlag = source.ArtivaRuleActiveFlag,
 target.ArtivaRuleGlobalFlag = source.ArtivaRuleGlobalFlag,
 target.ArtivaRuleSource = TRIM(source.ArtivaRuleSource),
 target.ArtivaRuleLastUpdateDate = source.ArtivaRuleLastUpdateDate,
 target.ArtivaRuleLastUpdateNote = TRIM(source.ArtivaRuleLastUpdateNote),
 target.ArtivaRuleLastUpdateBy = TRIM(source.ArtivaRuleLastUpdateBy),
 target.ArtivaPSENRELAYGUID = TRIM(source.ArtivaPSENRELAYGUID),
 target.ArtivaHoldCodeRiskReportFlag = TRIM(source.ArtivaHoldCodeRiskReportFlag),
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = source.InsertedBy,
 target.InsertedDTM = source.InsertedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (MonthID, SnapShotDate, LoadDateKey, ClaimKey, ClaimNumber, ClaimBalance, PracticeName, RegionKey, ArtivaRuleID, ArtivaRuleDesc, ArtivaRuleOwner, ArtivaRuleCategoryID, ArtivaHoldCodeID, ArtivaHoldCodeDescription, ArtivaHoldCodeType, ArtivaPpiIDPE, ArtivaPpiTypePE, ArtivaPpiIDEDI, ArtivaPpiTypeEDI, ArtivaPpiPhase, ArtivaRuleCreateDate, ArtivaRuleActiveFlag, ArtivaRuleGlobalFlag, ArtivaRuleSource, ArtivaRuleLastUpdateDate, ArtivaRuleLastUpdateNote, ArtivaRuleLastUpdateBy, ArtivaPSENRELAYGUID, ArtivaHoldCodeRiskReportFlag, SourceSystemCode, InsertedBy, InsertedDTM, DWLastUpdateDateTime)
  VALUES (source.MonthID, source.SnapShotDate, source.LoadDateKey, source.ClaimKey, source.ClaimNumber, source.ClaimBalance, TRIM(source.PracticeName), source.RegionKey, source.ArtivaRuleID, TRIM(source.ArtivaRuleDesc), TRIM(source.ArtivaRuleOwner), source.ArtivaRuleCategoryID, TRIM(source.ArtivaHoldCodeID), TRIM(source.ArtivaHoldCodeDescription), TRIM(source.ArtivaHoldCodeType), source.ArtivaPpiIDPE, TRIM(source.ArtivaPpiTypePE), source.ArtivaPpiIDEDI, TRIM(source.ArtivaPpiTypeEDI), TRIM(source.ArtivaPpiPhase), source.ArtivaRuleCreateDate, source.ArtivaRuleActiveFlag, source.ArtivaRuleGlobalFlag, TRIM(source.ArtivaRuleSource), source.ArtivaRuleLastUpdateDate, TRIM(source.ArtivaRuleLastUpdateNote), TRIM(source.ArtivaRuleLastUpdateBy), TRIM(source.ArtivaPSENRELAYGUID), TRIM(source.ArtivaHoldCodeRiskReportFlag), TRIM(source.SourceSystemCode), source.InsertedBy, source.InsertedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate, ClaimKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_FactSnapShotClaimsOnHold
      GROUP BY SnapShotDate, ClaimKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_FactSnapShotClaimsOnHold');
ELSE
  COMMIT TRANSACTION;
END IF;
