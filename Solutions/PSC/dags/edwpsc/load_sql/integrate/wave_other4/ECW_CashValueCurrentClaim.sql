
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_CashValueCurrentClaim;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_CashValueCurrentClaim
  (
    ClaimKey,
    ClaimNumber,
    RegionKey,
    ClaimBalanceCashValueAmt,
    DWLastUpdateDateTime)
SELECT
  source.ClaimKey,
  source.ClaimNumber,
  source.RegionKey,
  source.ClaimBalanceCashValueAmt,
  source.DWLastUpdateDateTime
FROM
  {{ params.param_psc_stage_dataset_name }}.ECW_CashValueCurrentClaim AS source;
