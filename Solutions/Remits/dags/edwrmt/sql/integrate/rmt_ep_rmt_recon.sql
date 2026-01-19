BEGIN TRANSACTION;

-- Translation time: 2025-03-04T09:56:02.533191Z
-- Translation job ID: b76396e3-185c-4254-a4c8-8cf665487226
-- Source: gs://eim-parallon-cs-datamig-dev-0002/clmrmt_bteqs_bulk_conversion/B1xZGH/input/rmt_ep_rmt_recon.sql
-- Translated from: Teradata
-- Translated to: BigQuery

DELETE FROM {{ params.param_rmt_core_dataset_name }}.ep_rmt_recon AS main
WHERE (hcaremitdt, unitnum, patacctnum) IN
(
SELECT
STRUCT(hcaremitdt, unitnum, patacctnum)
FROM
{{ params.param_rmt_stage_dataset_name }}.ep_rmt_recon
GROUP BY hcaremitdt, unitnum, patacctnum
HAVING COUNT(*) > 1
);

MERGE INTO {{ params.param_rmt_core_dataset_name }}.ep_rmt_recon AS main 
USING {{ params.param_rmt_stage_dataset_name }}.ep_rmt_recon AS stg 
ON UPPER(TRIM(main.patacctnum, ' ')) = UPPER(TRIM(stg.patacctnum, ' '))
AND UPPER(TRIM(main.unitnum, ' ')) = UPPER(TRIM(stg.unitnum, ' '))
AND main.hcaremitdt = stg.hcaremitdt 
WHEN MATCHED THEN
UPDATE
SET remitprovnpi = TRIM(stg.remitprovnpi),
    coid = TRIM(stg.coid),
    patientname = TRIM(stg.patientname),
    claimpaymentamt = stg.claimpaymentamt,
    paymentplbamt = stg.paymentplbamt,
    paymentchknum = TRIM(stg.paymentchknum),
    paymentchkamt = stg.paymentchkamt,
    payerbatchlabel = TRIM(stg.payerbatchlabel),
    claimbilldt = stg.claimbilldt,
    paymentchkdt = stg.paymentchkdt,
    payericn = TRIM(stg.payericn),
    claimcontrolmsg = TRIM(stg.claimcontrolmsg),
    iplan = TRIM(stg.iplan),
    eppayernum = TRIM(stg.eppayernum),
    overflow = TRIM(stg.overflow) 
WHEN NOT MATCHED BY TARGET THEN
INSERT (hcaremitdt,
        remitprovnpi,
        coid,
        unitnum,
        patientname,
        claimpaymentamt,
        patacctnum,
        paymentplbamt,
        paymentchknum,
        paymentchkamt,
        payerbatchlabel,
        claimbilldt,
        paymentchkdt,
        payericn,
        claimcontrolmsg,
        iplan,
        eppayernum,
        overflow)
VALUES (stg.hcaremitdt, TRIM(stg.remitprovnpi), TRIM(stg.coid), TRIM(stg.unitnum), TRIM(stg.patientname), 
stg.claimpaymentamt, TRIM(stg.patacctnum), stg.paymentplbamt, TRIM(stg.paymentchknum), stg.paymentchkamt, 
TRIM(stg.payerbatchlabel), stg.claimbilldt, stg.paymentchkdt, TRIM(stg.payericn), TRIM(stg.claimcontrolmsg), 
TRIM(stg.iplan), TRIM(stg.eppayernum), TRIM(stg.overflow));


COMMIT TRANSACTION;