
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.CMT_FactPplBalancing ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.CMT_FactPplBalancing
(PPLBalancinglKey, TransactionType, PostedDate, Coid, Siteid, PostedSystemTypeId, ArSystem, PplSumSummary, TotalAmountPostedIneCW, TotalAmountPostedInCmt, Variance, Specialist, Notes, Reasons, DWLastUpdateDateTime)
SELECT PPLBalancinglKey, TRIM(TransactionType) AS TransactionType, CAST(PostedDate AS DATE) AS PostedDate, TRIM(COID) AS COID, Siteid, PostedSystemTypeId, TRIM(ArSystem) AS ArSystem, TRIM(PplSumSummary) AS PplSumSummary, TotalAmountPostedIneCW, TotalAmountPostedInCmt, Variance, TRIM(Specialist) AS Specialist, TRIM(Notes) AS Notes, TRIM(Reasons) AS Reasons, CAST(DWLastUpdateDateTime AS DATETIME) AS DWLastUpdateDateTime
FROM {{ params.param_psc_stage_dataset_name }}.CMT_FactPplBalancing as source;