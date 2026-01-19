TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.CMT_FactEDPR ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.CMT_FactEDPR
(ReportId, ReportName, COID, ServiceDate, PracticeName, LOCATION, ReportStatus, ReportStatusId, Bank, EndDate, HasDeposit, DeleteIsAllowed, DWLastUpdateDateTime, BatchExceptionsDetailKey, LastSaved, BatchNbr, CreatedDate)
SELECT ReportId, TRIM(ReportName) AS ReportName, TRIM(COID) AS COID, CAST(ServiceDate AS DATE) AS ServiceDate, TRIM(PracticeName) AS PracticeName, TRIM(Location) AS Location, TRIM(ReportStatus) AS ReportStatus, ReportStatusId, Bank, CAST(EndDate AS DATE) AS EndDate, HasDeposit, DeleteIsAllowed, CAST(DWLastUpdateDateTime AS DATETIME) AS DWLastUpdateDateTime, BatchExceptionsDetailKey, CAST(LastSaved AS DATETIME) AS LastSaved, TRIM(BatchNbr) AS BatchNbr, CAST(CreatedDate AS DATETIME) AS CreatedDate
FROM {{ params.param_psc_stage_dataset_name }}.CMT_FactEDPR as source;