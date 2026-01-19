
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.Onbase_FactRefund AS target
USING {{ params.param_psc_stage_dataset_name }}.Onbase_FactRefund AS source
ON target.OnbaseRefundDataKey = source.OnbaseRefundDataKey
WHEN MATCHED THEN
  UPDATE SET
  target.DocumentHandle = source.DocumentHandle,
 target.TotalRefundAmount = TRIM(source.TotalRefundAmount),
 target.COID = TRIM(source.COID),
 target.ClaimNumber = TRIM(source.ClaimNumber),
 target.RefundAmount = TRIM(source.RefundAmount),
 target.RefundType = TRIM(source.RefundType),
 target.RefundSource = TRIM(source.RefundSource),
 target.ReasonForRefund = TRIM(source.ReasonForRefund),
 target.PayableTo = TRIM(source.PayableTo),
 target.TeamLeadApproval = TRIM(source.TeamLeadApproval),
 target.MgrApprovalUsername = TRIM(source.MgrApprovalUsername),
 target.DirectorApprovalUsername = TRIM(source.DirectorApprovalUsername),
 target.RequestedByDate = TRIM(source.RequestedByDate),
 target.AdjustmentCOde = TRIM(source.AdjustmentCOde),
 target.OtherApprovalUsername = TRIM(source.OtherApprovalUsername),
 target.RequestedBy = TRIM(source.RequestedBy),
 target.DateOfFinalApproval = TRIM(source.DateOfFinalApproval),
 target.NumberOfClaims = TRIM(source.NumberOfClaims),
 target.Status = TRIM(source.Status),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.RequestedByThreeFour = TRIM(source.RequestedByThreeFour),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.Region = source.Region,
 target.OnbaseRefundDataKey = source.OnbaseRefundDataKey
WHEN NOT MATCHED THEN
  INSERT (DocumentHandle, TotalRefundAmount, COID, ClaimNumber, RefundAmount, RefundType, RefundSource, ReasonForRefund, PayableTo, TeamLeadApproval, MgrApprovalUsername, DirectorApprovalUsername, RequestedByDate, AdjustmentCOde, OtherApprovalUsername, RequestedBy, DateOfFinalApproval, NumberOfClaims, Status, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, RequestedByThreeFour, DWLastUpdateDateTime, Region, OnbaseRefundDataKey)
  VALUES (source.DocumentHandle, TRIM(source.TotalRefundAmount), TRIM(source.COID), TRIM(source.ClaimNumber), TRIM(source.RefundAmount), TRIM(source.RefundType), TRIM(source.RefundSource), TRIM(source.ReasonForRefund), TRIM(source.PayableTo), TRIM(source.TeamLeadApproval), TRIM(source.MgrApprovalUsername), TRIM(source.DirectorApprovalUsername), TRIM(source.RequestedByDate), TRIM(source.AdjustmentCOde), TRIM(source.OtherApprovalUsername), TRIM(source.RequestedBy), TRIM(source.DateOfFinalApproval), TRIM(source.NumberOfClaims), TRIM(source.Status), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.RequestedByThreeFour), source.DWLastUpdateDateTime, source.Region, source.OnbaseRefundDataKey);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT OnbaseRefundDataKey
      FROM {{ params.param_psc_core_dataset_name }}.Onbase_FactRefund
      GROUP BY OnbaseRefundDataKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.Onbase_FactRefund');
ELSE
  COMMIT TRANSACTION;
END IF;
