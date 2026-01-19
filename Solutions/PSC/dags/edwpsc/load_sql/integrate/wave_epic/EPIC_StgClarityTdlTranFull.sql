
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_StgClarityTdlTranFull AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_StgClarityTdlTranFull AS source
ON target.TDL_ID = source.TDL_ID AND target.RegionKey = source.RegionKey
WHEN MATCHED THEN
  UPDATE SET
  target.TDL_ID = source.TDL_ID,
 target.DETAIL_TYPE = source.DETAIL_TYPE,
 target.TYPE = source.TYPE,
 target.POST_DATE = source.POST_DATE,
 target.ORIG_POST_DATE = source.ORIG_POST_DATE,
 target.ORIG_SERVICE_DATE = source.ORIG_SERVICE_DATE,
 target.PERIOD = TRIM(source.PERIOD),
 target.TX_ID = source.TX_ID,
 target.TRAN_TYPE = source.TRAN_TYPE,
 target.ALLOWED_AMOUNT = source.ALLOWED_AMOUNT,
 target.CHARGE_SLIP_NUMBER = TRIM(source.CHARGE_SLIP_NUMBER),
 target.TYPE_OF_SERVICE = source.TYPE_OF_SERVICE,
 target.MATCH_TRX_ID = source.MATCH_TRX_ID,
 target.MATCH_PROC_ID = source.MATCH_PROC_ID,
 target.ACCOUNT_ID = source.ACCOUNT_ID,
 target.PAT_ID = TRIM(source.PAT_ID),
 target.AMOUNT = source.AMOUNT,
 target.PATIENT_AMOUNT = source.PATIENT_AMOUNT,
 target.INSURANCE_AMOUNT = source.INSURANCE_AMOUNT,
 target.PERFORMING_PROV_ID = TRIM(source.PERFORMING_PROV_ID),
 target.BILLING_PROVIDER_ID = TRIM(source.BILLING_PROVIDER_ID),
 target.ORIGINAL_CVG_ID = source.ORIGINAL_CVG_ID,
 target.PROC_ID = source.PROC_ID,
 target.PROCEDURE_QUANTITY = source.PROCEDURE_QUANTITY,
 target.MODIFIER_ONE = TRIM(source.MODIFIER_ONE),
 target.MODIFIER_TWO = TRIM(source.MODIFIER_TWO),
 target.MODIFIER_THREE = TRIM(source.MODIFIER_THREE),
 target.MODIFIER_FOUR = TRIM(source.MODIFIER_FOUR),
 target.DX_ONE_ID = source.DX_ONE_ID,
 target.DX_TWO_ID = source.DX_TWO_ID,
 target.DX_THREE_ID = source.DX_THREE_ID,
 target.DX_FOUR_ID = source.DX_FOUR_ID,
 target.DX_FIVE_ID = source.DX_FIVE_ID,
 target.DX_SIX_ID = source.DX_SIX_ID,
 target.SERV_AREA_ID = source.SERV_AREA_ID,
 target.LOC_ID = source.LOC_ID,
 target.DEPT_ID = source.DEPT_ID,
 target.POS_ID = source.POS_ID,
 target.ACTION_CVG_ID = source.ACTION_CVG_ID,
 target.USER_ID = TRIM(source.USER_ID),
 target.TX_NUM = source.TX_NUM,
 target.INT_PAT_ID = TRIM(source.INT_PAT_ID),
 target.REFERRAL_SOURCE_ID = TRIM(source.REFERRAL_SOURCE_ID),
 target.PAYMENT_SOURCE_C = source.PAYMENT_SOURCE_C,
 target.VISIT_NUMBER = TRIM(source.VISIT_NUMBER),
 target.PAT_ENC_CSN_ID = source.PAT_ENC_CSN_ID,
 target.ACTION_USER_ID = TRIM(source.ACTION_USER_ID),
 target.HSP_ACCOUNT_ID = source.HSP_ACCOUNT_ID,
 target.BILL_AREA_ID = source.BILL_AREA_ID,
 target.SourceCPrimaryKeyValue = TRIM(source.SourceCPrimaryKeyValue),
 target.RegionKey = source.RegionKey,
 target.TX_COMMENT = TRIM(source.TX_COMMENT),
 target.DWLastUpdated = source.DWLastUpdated,
 target.DIST_USER_ID = TRIM(source.DIST_USER_ID),
 target.UNDIST_USER_ID = TRIM(source.UNDIST_USER_ID),
 target.CUR_PAYOR_ID = source.CUR_PAYOR_ID,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (TDL_ID, DETAIL_TYPE, TYPE, POST_DATE, ORIG_POST_DATE, ORIG_SERVICE_DATE, PERIOD, TX_ID, TRAN_TYPE, ALLOWED_AMOUNT, CHARGE_SLIP_NUMBER, TYPE_OF_SERVICE, MATCH_TRX_ID, MATCH_PROC_ID, ACCOUNT_ID, PAT_ID, AMOUNT, PATIENT_AMOUNT, INSURANCE_AMOUNT, PERFORMING_PROV_ID, BILLING_PROVIDER_ID, ORIGINAL_CVG_ID, PROC_ID, PROCEDURE_QUANTITY, MODIFIER_ONE, MODIFIER_TWO, MODIFIER_THREE, MODIFIER_FOUR, DX_ONE_ID, DX_TWO_ID, DX_THREE_ID, DX_FOUR_ID, DX_FIVE_ID, DX_SIX_ID, SERV_AREA_ID, LOC_ID, DEPT_ID, POS_ID, ACTION_CVG_ID, USER_ID, TX_NUM, INT_PAT_ID, REFERRAL_SOURCE_ID, PAYMENT_SOURCE_C, VISIT_NUMBER, PAT_ENC_CSN_ID, ACTION_USER_ID, HSP_ACCOUNT_ID, BILL_AREA_ID, SourceCPrimaryKeyValue, RegionKey, TX_COMMENT, DWLastUpdated, DIST_USER_ID, UNDIST_USER_ID, CUR_PAYOR_ID, DWLastUpdateDateTime)
  VALUES (source.TDL_ID, source.DETAIL_TYPE, source.TYPE, source.POST_DATE, source.ORIG_POST_DATE, source.ORIG_SERVICE_DATE, TRIM(source.PERIOD), source.TX_ID, source.TRAN_TYPE, source.ALLOWED_AMOUNT, TRIM(source.CHARGE_SLIP_NUMBER), source.TYPE_OF_SERVICE, source.MATCH_TRX_ID, source.MATCH_PROC_ID, source.ACCOUNT_ID, TRIM(source.PAT_ID), source.AMOUNT, source.PATIENT_AMOUNT, source.INSURANCE_AMOUNT, TRIM(source.PERFORMING_PROV_ID), TRIM(source.BILLING_PROVIDER_ID), source.ORIGINAL_CVG_ID, source.PROC_ID, source.PROCEDURE_QUANTITY, TRIM(source.MODIFIER_ONE), TRIM(source.MODIFIER_TWO), TRIM(source.MODIFIER_THREE), TRIM(source.MODIFIER_FOUR), source.DX_ONE_ID, source.DX_TWO_ID, source.DX_THREE_ID, source.DX_FOUR_ID, source.DX_FIVE_ID, source.DX_SIX_ID, source.SERV_AREA_ID, source.LOC_ID, source.DEPT_ID, source.POS_ID, source.ACTION_CVG_ID, TRIM(source.USER_ID), source.TX_NUM, TRIM(source.INT_PAT_ID), TRIM(source.REFERRAL_SOURCE_ID), source.PAYMENT_SOURCE_C, TRIM(source.VISIT_NUMBER), source.PAT_ENC_CSN_ID, TRIM(source.ACTION_USER_ID), source.HSP_ACCOUNT_ID, source.BILL_AREA_ID, TRIM(source.SourceCPrimaryKeyValue), source.RegionKey, TRIM(source.TX_COMMENT), source.DWLastUpdated, TRIM(source.DIST_USER_ID), TRIM(source.UNDIST_USER_ID), source.CUR_PAYOR_ID, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT TDL_ID, RegionKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_StgClarityTdlTranFull
      GROUP BY TDL_ID, RegionKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_StgClarityTdlTranFull');
ELSE
  COMMIT TRANSACTION;
END IF;
