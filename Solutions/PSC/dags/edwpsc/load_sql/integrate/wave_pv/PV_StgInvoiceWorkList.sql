DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgInvoiceWorkList AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgInvoiceWorkList AS source
ON 
    target.InvNum = source.InvNum AND 
    target.ARCtrlNum = source.ARCtrlNum AND 
    target.Practice = source.Practice AND
    target.RegionKey = source.RegionKey
WHEN MATCHED THEN UPDATE SET
    target.InvoiceWorkListId = source.InvoiceWorkListId,
    target.Practice = TRIM(source.Practice),
    target.ARCtrlNum = source.ARCtrlNum,
    target.InvNum = source.InvNum,
    target.Clinic = TRIM(source.Clinic),
    target.SvcDate = source.SvcDate,
    target.InvDate = source.InvDate,
    target.VisitType = TRIM(source.VisitType),
    target.PatNum = source.PatNum,
    target.PatName = TRIM(source.PatName),
    target.PayerNum = source.PayerNum,
    target.PayerClass = TRIM(source.PayerClass),
    target.PayerType = TRIM(source.PayerType),
    target.PayerName = TRIM(source.PayerName),
    target.CrgBalance = source.CrgBalance,
    target.ActionDate = source.ActionDate,
    target.WorkListPK = TRIM(source.WorkListPK),
    target.ReleaseFlag = TRIM(source.ReleaseFlag),
    target.DisplayUI = source.DisplayUI,
    target.RegionKey = source.RegionKey,
    target.InsertedDTM = source.InsertedDTM,
    target.ModifiedDTM = source.ModifiedDTM,
    target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
    target.SourcePhysicalDeleteFlag = source.SourcePhysicalDeleteFlag
WHEN NOT MATCHED THEN INSERT 
(
    InvoiceWorkListId, 
    Practice, 
    ARCtrlNum, 
    InvNum, 
    Clinic, 
    SvcDate, 
    InvDate, 
    VisitType, 
    PatNum, 
    PatName, 
    PayerNum, 
    PayerClass, 
    PayerType, 
    PayerName, 
    CrgBalance, 
    ActionDate, 
    WorkListPK, 
    ReleaseFlag, 
    DisplayUI, 
    RegionKey, 
    InsertedDTM, 
    ModifiedDTM, 
    DWLastUpdateDateTime, 
    SourcePhysicalDeleteFlag
)
VALUES 
(
    source.InvoiceWorkListId, 
    TRIM(source.Practice), 
    source.ARCtrlNum, 
    source.InvNum, 
    TRIM(source.Clinic), 
    source.SvcDate, 
    source.InvDate, 
    TRIM(source.VisitType), 
    source.PatNum, 
    TRIM(source.PatName), 
    source.PayerNum, 
    TRIM(source.PayerClass), 
    TRIM(source.PayerType), 
    TRIM(source.PayerName), 
    source.CrgBalance, 
    source.ActionDate, 
    TRIM(source.WorkListPK), 
    TRIM(source.ReleaseFlag), 
    source.DisplayUI, 
    source.RegionKey, 
    source.InsertedDTM, 
    source.ModifiedDTM, 
    source.DWLastUpdateDateTime, 
    source.SourcePhysicalDeleteFlag
);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT InvNum, ARCtrlNum, Practice, RegionKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgInvoiceWorkList
      GROUP BY InvNum, ARCtrlNum, Practice, RegionKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgLabOrder');
ELSE
  COMMIT TRANSACTION;
END IF;

