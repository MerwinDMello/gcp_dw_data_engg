
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RprtBillingCoodinatorProductivity;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RprtBillingCoodinatorProductivity
  (
    WeekStartDate,
    WeekEndDate,
    SavedBy,
    ActionDate,
    MONTH,
    NextTouchToCcu,
    NextTouchToOutBox,
    ClaimSubStatusName,
    COCID,
    TouchCount,
    SourceSystem,
    InsertedDTM,
    DWLastUpdateDateTime,
    BillingCoordinatorProdKey)
SELECT
  source.WeekStartDate,
  source.WeekEndDate,
  TRIM(source.SavedBy),
  source.ActionDate,
  source.MONTH,
  TRIM(source.NextTouchToCcu),
  TRIM(source.NextTouchToOutBox),
  TRIM(source.ClaimSubStatusName),
  TRIM(source.COCID),
  source.TouchCount,
  TRIM(source.SourceSystem),
  source.InsertedDTM,
  source.DWLastUpdateDateTime,
  source.BillingCoordinatorProdKey
FROM
  {{ params.param_psc_stage_dataset_name }}.ECW_RprtBillingCoodinatorProductivity
    AS source;