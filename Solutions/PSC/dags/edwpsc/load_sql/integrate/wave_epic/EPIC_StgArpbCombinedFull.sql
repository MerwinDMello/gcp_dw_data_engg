
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_StgArpbCombinedFull AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_StgArpbCombinedFull AS source
ON target.TX_ID = source.TX_ID AND target.RegionKey = source.RegionKey
WHEN MATCHED THEN
  UPDATE SET
  target.TX_ID = source.TX_ID,
 target.VOID_DATE = source.VOID_DATE,
 target.POST_DATE = source.POST_DATE,
 target.CREDIT_SRC_MODULE_C = source.CREDIT_SRC_MODULE_C,
 target.PAYMENT_SOURCE_C = source.PAYMENT_SOURCE_C,
 target.COVERAGE_ID = source.COVERAGE_ID,
 target.IS_RETRO_TX = TRIM(source.IS_RETRO_TX),
 target.IS_REVERSED_C = source.IS_REVERSED_C,
 target.REPOSTED_VOID_FLAG = source.REPOSTED_VOID_FLAG,
 target.Post_Batch_Num = TRIM(source.Post_Batch_Num),
 target.Reference_Num = TRIM(source.Reference_Num),
 target.COLL_AGENCY_NAME = TRIM(source.COLL_AGENCY_NAME),
 target.SNAP_START_DATE = source.SNAP_START_DATE,
 target.AMOUNT = source.AMOUNT,
 target.RegionKey = source.RegionKey,
 target.EXT_AGNCY_SENT_DTTM = source.EXT_AGNCY_SENT_DTTM,
 target.DEBIT_CREDIT_FLAG = source.DEBIT_CREDIT_FLAG,
 target.DWLastUpdated = source.DWLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (TX_ID, VOID_DATE, POST_DATE, CREDIT_SRC_MODULE_C, PAYMENT_SOURCE_C, COVERAGE_ID, IS_RETRO_TX, IS_REVERSED_C, REPOSTED_VOID_FLAG, Post_Batch_Num, Reference_Num, COLL_AGENCY_NAME, SNAP_START_DATE, AMOUNT, RegionKey, EXT_AGNCY_SENT_DTTM, DEBIT_CREDIT_FLAG, DWLastUpdated, DWLastUpdateDateTime)
  VALUES (source.TX_ID, source.VOID_DATE, source.POST_DATE, source.CREDIT_SRC_MODULE_C, source.PAYMENT_SOURCE_C, source.COVERAGE_ID, TRIM(source.IS_RETRO_TX), source.IS_REVERSED_C, source.REPOSTED_VOID_FLAG, TRIM(source.Post_Batch_Num), TRIM(source.Reference_Num), TRIM(source.COLL_AGENCY_NAME), source.SNAP_START_DATE, source.AMOUNT, source.RegionKey, source.EXT_AGNCY_SENT_DTTM, source.DEBIT_CREDIT_FLAG, source.DWLastUpdated, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT TX_ID, RegionKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_StgArpbCombinedFull
      GROUP BY TX_ID, RegionKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_StgArpbCombinedFull');
ELSE
  COMMIT TRANSACTION;
END IF;
