
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactSnapShotClaimsOnHold AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactSnapShotClaimsOnHold AS source
ON target.snapshotdate = source.snapshotdate AND target.claimkey = source.claimkey
WHEN MATCHED THEN
  UPDATE SET
  target.MonthId = source.MonthId,
 target.SnapShotDate = source.SnapShotDate,
 target.LoadDateKey = source.LoadDateKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.ClaimBalance = source.ClaimBalance,
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
 target.ArtivaHoldCodeRiskReportFlag = TRIM(source.ArtivaHoldCodeRiskReportFlag),
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM
WHEN NOT MATCHED THEN
  INSERT (MonthId, SnapShotDate, LoadDateKey, ClaimKey, ClaimNumber, ClaimBalance, RegionKey, ArtivaRuleID, ArtivaRuleDesc, ArtivaRuleOwner, ArtivaRuleCategoryID, ArtivaHoldCodeID, ArtivaHoldCodeDescription, ArtivaHoldCodeType, ArtivaPpiIDPE, ArtivaPpiTypePE, ArtivaPpiIDEDI, ArtivaPpiTypeEDI, ArtivaPpiPhase, ArtivaRuleCreateDate, ArtivaRuleActiveFlag, ArtivaRuleGlobalFlag, ArtivaRuleSource, ArtivaRuleLastUpdateDate, ArtivaRuleLastUpdateNote, ArtivaRuleLastUpdateBy, ArtivaHoldCodeRiskReportFlag, SourceSystemCode, InsertedBy, InsertedDTM)
  VALUES (source.MonthId, source.SnapShotDate, source.LoadDateKey, source.ClaimKey, source.ClaimNumber, source.ClaimBalance, source.RegionKey, source.ArtivaRuleID, TRIM(source.ArtivaRuleDesc), TRIM(source.ArtivaRuleOwner), source.ArtivaRuleCategoryID, TRIM(source.ArtivaHoldCodeID), TRIM(source.ArtivaHoldCodeDescription), TRIM(source.ArtivaHoldCodeType), source.ArtivaPpiIDPE, TRIM(source.ArtivaPpiTypePE), source.ArtivaPpiIDEDI, TRIM(source.ArtivaPpiTypeEDI), TRIM(source.ArtivaPpiPhase), source.ArtivaRuleCreateDate, source.ArtivaRuleActiveFlag, source.ArtivaRuleGlobalFlag, TRIM(source.ArtivaRuleSource), source.ArtivaRuleLastUpdateDate, TRIM(source.ArtivaRuleLastUpdateNote), TRIM(source.ArtivaRuleLastUpdateBy), TRIM(source.ArtivaHoldCodeRiskReportFlag), TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT snapshotdate, claimkey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactSnapShotClaimsOnHold
      GROUP BY snapshotdate, claimkey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactSnapShotClaimsOnHold');
ELSE
  COMMIT TRANSACTION;
END IF;
