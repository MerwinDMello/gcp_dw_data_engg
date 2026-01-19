
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.CCU_RprtProductivity ;

INSERT INTO
  {{ params.param_psc_core_dataset_name }}.CCU_RprtProductivity (Coder34Id,
    TouchDateKey,
    EOMonthDate,
    SourceSystemCode,
    NotInventoryClaims,
    NotInventoryCpts,
    InventoryClaims,
    InventoryCpts,
    StatusChanges,
    CoidCount,
    DWLastUpdateDateTime)
SELECT
  TRIM(source.Coder34Id),
  source.TouchDateKey,
  source.EOMonthDate,
  TRIM(source.SourceSystemCode),
  source.NotInventoryClaims,
  source.NotInventoryCpts,
  source.InventoryClaims,
  source.InventoryCpts,
  source.StatusChanges,
  source.CoidCount,
  source.DWLastUpdateDateTime
FROM
  {{ params.param_psc_stage_dataset_name }}.CCU_RprtProductivity AS SOURCE;
