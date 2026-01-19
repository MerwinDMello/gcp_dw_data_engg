TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.CMT_FactBatchExceptionsDetail ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.CMT_FactBatchExceptionsDetail
(BatchId, BatchDate, DepositDate, LocationName, LocationCode, Notes, TransactionTypeID, TransactionType, DepositAmount, PostAmount, Variance, COID, Specialist, StatusNotes, Age, BatchAge, BatchReasonID, WorkflowStatus, SpecialistID, BatchExceptionsDetailKey, DWLastUpdateDateTime, SelectedCOIDUser)
SELECT TRIM(BatchId) AS BatchId, CAST(BatchDate AS DATETIME) AS BatchDate, TRIM(DepositDate) AS DepositDate, TRIM(LocationName) AS LocationName, TRIM(LocationCode) AS LocationCode, TRIM(Notes) AS Notes, TransactionTypeID, TRIM(TransactionType) AS TransactionType, DepositAmount, PostAmount, Variance, TRIM(COID) AS COID, TRIM(Specialist) AS Specialist, TRIM(StatusNotes) AS StatusNotes, Age, BatchAge, TRIM(BatchReasonID) AS BatchReasonID, TRIM(WorkflowStatus) AS WorkflowStatus, SpecialistID, BatchExceptionsDetailKey, CAST(DWLastUpdateDateTime AS DATETIME) AS DWLastUpdateDateTime, TRIM(SelectedCOIDUser) AS SelectedCOIDUser
FROM {{ params.param_psc_stage_dataset_name }}.CMT_FactBatchExceptionsDetail as source;
