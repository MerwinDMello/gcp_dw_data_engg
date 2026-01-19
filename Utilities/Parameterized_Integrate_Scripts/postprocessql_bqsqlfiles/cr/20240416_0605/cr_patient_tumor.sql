DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cr_patient_tumor.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #####################################################################################
-- #  TARGET TABLE: EDWCR.CR_PATIENT_TUMOR             #
-- #  TARGET  DATABASE   : EDWCR #
-- #  SOURCE   : ${NCR_STG_SCHEMA}.CR_Patient_Tumor_STG#
-- #                                                                        #
-- #  INITIAL RELEASE   : #
-- #  PROJECT             :
-- #  Created by:       Amit Singh    #
-- #  ------------------------------------------------------------------------#
-- #                                                                              #
-- #####################################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_PATIENT_TUMOR;;
 --' FOR SESSION;;
 /* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_STAGING','CR_Patient_Tumor_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_STAGING','Ref_LookUp_Groups_Stg');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_lkp CLUSTER BY lookup_code AS
SELECT rln.lookup_sid,
       rlc.lookup_code,
       rlc.lookup_sub_code,
       rlc.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
WHERE upper(rtrim(rln.lookup_name)) = 'GRADE';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- COLLECT STATISTICS is not supported in this dialect.
BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_lkp1 CLUSTER BY lookup_code AS
SELECT rln.lookup_sid,
       rlc.lookup_code,
       rlc.lookup_sub_code,
       rlc.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
WHERE upper(rtrim(rln.lookup_name)) = 'CASE STATUS';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- COLLECT STATISTICS is not supported in this dialect.
BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_lkp2 CLUSTER BY lookup_code AS
SELECT rln.lookup_sid,
       rlc.lookup_code,
       rlc.lookup_sub_code,
       rlc.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
WHERE upper(rtrim(rln.lookup_name)) = 'PRIMARY SITE';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- COLLECT STATISTICS is not supported in this dialect.
BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_lkp3 CLUSTER BY lookup_code AS
SELECT rln.lookup_sid,
       rlc.lookup_code,
       rlc.lookup_sub_code,
       rlc.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
WHERE upper(rtrim(rln.lookup_name)) = 'CHEMO SUMMARY';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- COLLECT STATISTICS is not supported in this dialect.
BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_lkp4 CLUSTER BY lookup_code AS
SELECT rln.lookup_sid,
       rlc.lookup_code,
       rlc.lookup_sub_code,
       rlc.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
WHERE upper(rtrim(rln.lookup_name)) = 'DIAGNOSTIC STAGE SUMMARY';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- COLLECT STATISTICS is not supported in this dialect.
BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_lkp5 CLUSTER BY lookup_code AS
SELECT rln.lookup_sid,
       rlc.lookup_code,
       rlc.lookup_sub_code,
       rlc.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
WHERE upper(rtrim(rln.lookup_name)) = 'HORMONE SUMMARY';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- COLLECT STATISTICS is not supported in this dialect.
BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_lkp6 CLUSTER BY lookup_code AS
SELECT rln.lookup_sid,
       rlc.lookup_code,
       rlc.lookup_sub_code,
       rlc.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
WHERE upper(rtrim(rln.lookup_name)) = 'IMMUNO SUMMARY';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- COLLECT STATISTICS is not supported in this dialect.
BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_lkp7 CLUSTER BY lookup_code AS
SELECT rln.lookup_sid,
       rlc.lookup_code,
       rlc.lookup_sub_code,
       rlc.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
WHERE upper(rtrim(rln.lookup_name)) = 'SURGICAL MARGIN SUMMARY';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- COLLECT STATISTICS is not supported in this dialect.
BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_lkp8 CLUSTER BY lookup_code AS
SELECT rln.lookup_sid,
       rlc.lookup_code,
       rlc.lookup_sub_code,
       rlc.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
WHERE upper(rtrim(rln.lookup_name)) = 'PALLIATIVE CARE SUMMARY';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- COLLECT STATISTICS is not supported in this dialect.
BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_lkp9 CLUSTER BY lookup_code AS
SELECT rln.lookup_sid,
       rlc.lookup_code,
       rlc.lookup_sub_code,
       rlc.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
WHERE upper(rtrim(rln.lookup_name)) = 'RADIATION BOOST MODALITY';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- COLLECT STATISTICS is not supported in this dialect.
BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_lkp10 CLUSTER BY lookup_code AS
SELECT rln.lookup_sid,
       rlc.lookup_code,
       rlc.lookup_sub_code,
       rlc.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
WHERE upper(rtrim(rln.lookup_name)) = 'RADIATION HOSPITAL';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- COLLECT STATISTICS is not supported in this dialect.
BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_lkp11 CLUSTER BY lookup_code AS
SELECT rln.lookup_sid,
       rlc.lookup_code,
       rlc.lookup_sub_code,
       rlc.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
WHERE upper(rtrim(rln.lookup_name)) = 'RADIATION SITE';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- COLLECT STATISTICS is not supported in this dialect.
BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_lkp12 CLUSTER BY lookup_code AS
SELECT rln.lookup_sid,
       rlc.lookup_code,
       rlc.lookup_sub_code,
       rlc.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
WHERE upper(rtrim(rln.lookup_name)) = 'CHEMO DECLINE';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- COLLECT STATISTICS is not supported in this dialect.
BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_lkp13 CLUSTER BY lookup_code AS
SELECT rln.lookup_sid,
       rlc.lookup_code,
       rlc.lookup_sub_code,
       rlc.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
WHERE upper(rtrim(rln.lookup_name)) = 'HORMONE DECLINE';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- COLLECT STATISTICS is not supported in this dialect.
BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_lkp14 CLUSTER BY lookup_code AS
SELECT rln.lookup_sid,
       rlc.lookup_code,
       rlc.lookup_sub_code,
       rlc.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
WHERE upper(rtrim(rln.lookup_name)) = 'IMMUNO DECLINE';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- COLLECT STATISTICS is not supported in this dialect.
BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_lkp15 CLUSTER BY lookup_code AS
SELECT rln.lookup_sid,
       rlc.lookup_code,
       rlc.lookup_sub_code,
       rlc.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
WHERE upper(rtrim(rln.lookup_name)) = 'RADIATION DECLINE';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- COLLECT STATISTICS is not supported in this dialect.
BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_lkp16 CLUSTER BY lookup_code AS
SELECT rln.lookup_sid,
       rlc.lookup_code,
       rlc.lookup_sub_code,
       rlc.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
WHERE upper(rtrim(rln.lookup_name)) = 'PRIMARY SITE SURGERY SUMMARY';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- COLLECT STATISTICS is not supported in this dialect.
BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_lkp17 CLUSTER BY lookup_code AS
SELECT rln.lookup_sid,
       rlc.lookup_code,
       rlc.lookup_sub_code,
       rlc.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
WHERE upper(rtrim(rln.lookup_name)) = 'TREATMENT THERAPY SCHEDULE';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- COLLECT STATISTICS is not supported in this dialect.
BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_lkp18 CLUSTER BY lookup_code AS
SELECT rln.lookup_sid,
       rlc.lookup_code,
       rlc.lookup_sub_code,
       rlc.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
WHERE upper(rtrim(rln.lookup_name)) = 'RADIATION MODALITY';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- COLLECT STATISTICS is not supported in this dialect.
BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_lkp19 CLUSTER BY lookup_code AS
SELECT rln.lookup_sid,
       rlc.lookup_code,
       rlc.lookup_sub_code,
       rlc.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
WHERE upper(rtrim(rln.lookup_name)) = 'RADIATION VOLUME SUMMARY';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- COLLECT STATISTICS is not supported in this dialect.
BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_lkp20 CLUSTER BY lookup_code AS
SELECT rln.lookup_sid,
       rlc.lookup_code,
       rlc.lookup_sub_code,
       rlc.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
WHERE upper(rtrim(rln.lookup_name)) = 'REGIONAL LYMPH NODE SUMMARY';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_lkp21 CLUSTER BY lookup_code AS
SELECT rln.lookup_sid,
       rlc.lookup_code,
       rlc.lookup_sub_code,
       rlc.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
WHERE upper(rtrim(rln.lookup_name)) = 'SURGERY APPROACH HOSPITAL';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- COLLECT STATISTICS is not supported in this dialect.
BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_lkp22 CLUSTER BY lookup_code AS
SELECT rln.lookup_sid,
       rlc.lookup_code,
       rlc.lookup_sub_code,
       rlc.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
WHERE upper(rtrim(rln.lookup_name)) = 'PRIMARY SITE SURGERY HOSPITAL';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- COLLECT STATISTICS is not supported in this dialect.
BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_lkp23 CLUSTER BY lookup_code AS
SELECT rln.lookup_sid,
       rlc.lookup_code,
       rlc.lookup_sub_code,
       rlc.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
WHERE upper(rtrim(rln.lookup_name)) = 'TUMOR SIZE SUMMARY';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- COLLECT STATISTICS is not supported in this dialect.
BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_lkp24 CLUSTER BY lookup_code AS
SELECT rln.lookup_sid,
       rlc.lookup_code,
       rlc.lookup_sub_code,
       rlc.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
WHERE upper(rtrim(rln.lookup_name)) = 'COLLABORATIVE STAGE SUMMARY';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- COLLECT STATISTICS is not supported in this dialect.
BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_lkp25 CLUSTER BY lookup_code AS
SELECT rln.lookup_sid,
       rlc.lookup_code,
       rlc.lookup_sub_code,
       rlc.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
WHERE upper(rtrim(rln.lookup_name)) = 'COLLABORATIVE STAGE TNM';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- COLLECT STATISTICS is not supported in this dialect.
BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_lkp26 CLUSTER BY lookup_code AS
SELECT rln.lookup_sid,
       rlc.lookup_code,
       rlc.lookup_sub_code,
       rlc.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
WHERE upper(rtrim(rln.lookup_name)) = 'DEFINITIVE TREATMENT FACILITY';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- COLLECT STATISTICS is not supported in this dialect.
BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_lkp27 CLUSTER BY lookup_code AS
SELECT rln.lookup_sid,
       rlc.lookup_code,
       rlc.lookup_sub_code,
       rlc.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
WHERE upper(rtrim(rln.lookup_name)) = 'SECONDARY TREATMENT FACILITY';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- COLLECT STATISTICS is not supported in this dialect.
BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_lkp28 CLUSTER BY lookup_code AS
SELECT rln.lookup_sid,
       rlc.lookup_code,
       rlc.lookup_sub_code,
       rlc.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
WHERE upper(rtrim(rln.lookup_name)) = 'TERTIARY TREATMENT FACILITY';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- COLLECT STATISTICS is not supported in this dialect.
BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_lkp29 CLUSTER BY lookup_code AS
SELECT rln.lookup_sid,
       rlc.lookup_code,
       rlc.lookup_sub_code,
       rlc.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
WHERE upper(rtrim(rln.lookup_name)) = 'CLASS CASE';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- COLLECT STATISTICS is not supported in this dialect.
BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_lkp30 CLUSTER BY lookup_code AS
SELECT rln.lookup_sid,
       rlc.lookup_code,
       rlc.lookup_sub_code,
       rlc.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
WHERE upper(rtrim(rln.lookup_name)) = 'SEQUENCE PRIMARY';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- COLLECT STATISTICS is not supported in this dialect.
BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_lkp31 CLUSTER BY lookup_code AS
SELECT rlc.master_lookup_sid,
       rlc.lookup_code
FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
WHERE upper(rtrim(rln.lookup_name)) = 'PRESENTED TO CANCER CONFERENCE/MULTIPLE DISCIPLINARY MEETING';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- COLLECT STATISTICS is not supported in this dialect.
BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.cr_patient_tumor;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Populate Core  Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cr_patient_tumor AS mt USING
  (SELECT DISTINCT stg.patientid AS cr_patient_id,
                   stg.tumorid AS tumor_id,
                   coalesce(vt_lkp.master_lookup_sid,
                              (SELECT vt_lkp_0.master_lookup_sid
                               FROM vt_lkp AS vt_lkp_0
                               WHERE upper(rtrim(vt_lkp_0.lookup_code)) = '-99' )) AS tumor_grade_id,
                   coalesce(vt_lkp1.master_lookup_sid,
                              (SELECT vt_lkp1_0.master_lookup_sid
                               FROM vt_lkp1 AS vt_lkp1_0
                               WHERE upper(rtrim(vt_lkp1_0.lookup_code)) = '-99' )) AS case_status_id,
                   coalesce(vt_lkp2.master_lookup_sid,
                              (SELECT vt_lkp2_0.master_lookup_sid
                               FROM vt_lkp2 AS vt_lkp2_0
                               WHERE upper(rtrim(vt_lkp2_0.lookup_code)) = '-99' )) AS tumor_primary_site_id, -- Coalesce(vt_LKP3.Master_Lookup_Sid,(Select Master_Lookup_Sid from vt_LKP3 where Lookup_Code='-99')) as Definitive_Chemo_Summary_Id,
 CAST(NULL AS INT64) AS definitive_chemo_summary_id, -- Coalesce(vt_LKP4.Master_Lookup_Sid,(Select Master_Lookup_Sid from vt_LKP4 where Lookup_Code='-99')) as Definitive_Diagnostic_Stage_Summary_Id,
 CAST(NULL AS INT64) AS definitive_diagnostic_stage_summary_id, -- Coalesce(vt_LKP5.Master_Lookup_Sid,(Select Master_Lookup_Sid from vt_LKP5 where Lookup_Code='-99')) as Definitive_Hormone_Summary_Id,
 CAST(NULL AS INT64) AS definitive_hormone_summary_id, -- Coalesce(vt_LKP6.Master_Lookup_Sid,(Select Master_Lookup_Sid from vt_LKP6 where Lookup_Code='-99')) as Definitive_Immuno_Summary_Id,
 CAST(NULL AS INT64) AS definitive_immuno_summary_id,
 coalesce(vt_lkp7.master_lookup_sid,
            (SELECT vt_lkp7_0.master_lookup_sid
             FROM vt_lkp7 AS vt_lkp7_0
             WHERE upper(rtrim(vt_lkp7_0.lookup_code)) = '-99' )) AS definitive_surgical_margin_summary_id, -- Coalesce(vt_LKP8.Master_Lookup_Sid,(Select Master_Lookup_Sid from vt_LKP8 where Lookup_Code='-99')) as Definitive_Palliative_Care_Summary_Id,
 CAST(NULL AS INT64) AS definitive_palliative_care_summary_id,
 coalesce(vt_lkp9.master_lookup_sid,
            (SELECT vt_lkp9_0.master_lookup_sid
             FROM vt_lkp9 AS vt_lkp9_0
             WHERE upper(rtrim(vt_lkp9_0.lookup_code)) = '-99' )) AS radiation_boost_modality_id,
 coalesce(vt_lkp10.master_lookup_sid,
            (SELECT vt_lkp10_0.master_lookup_sid
             FROM vt_lkp10 AS vt_lkp10_0
             WHERE upper(rtrim(vt_lkp10_0.lookup_code)) = '-99' )) AS radiation_hospital_id,
 coalesce(vt_lkp11.master_lookup_sid,
            (SELECT vt_lkp11_0.master_lookup_sid
             FROM vt_lkp11 AS vt_lkp11_0
             WHERE upper(rtrim(vt_lkp11_0.lookup_code)) = '-99' )) AS radiation_site_id,
 coalesce(vt_lkp12.master_lookup_sid,
            (SELECT vt_lkp12_0.master_lookup_sid
             FROM vt_lkp12 AS vt_lkp12_0
             WHERE upper(rtrim(vt_lkp12_0.lookup_code)) = '-99' )) AS chemo_declined_reason_id,
 coalesce(vt_lkp13.master_lookup_sid,
            (SELECT vt_lkp13_0.master_lookup_sid
             FROM vt_lkp13 AS vt_lkp13_0
             WHERE upper(rtrim(vt_lkp13_0.lookup_code)) = '-99' )) AS hormone_declined_reason_id,
 coalesce(vt_lkp14.master_lookup_sid,
            (SELECT vt_lkp14_0.master_lookup_sid
             FROM vt_lkp14 AS vt_lkp14_0
             WHERE upper(rtrim(vt_lkp14_0.lookup_code)) = '-99' )) AS immuno_declined_reason_id,
 coalesce(vt_lkp15.master_lookup_sid,
            (SELECT vt_lkp15_0.master_lookup_sid
             FROM vt_lkp15 AS vt_lkp15_0
             WHERE upper(rtrim(vt_lkp15_0.lookup_code)) = '-99' )) AS radiation_declined_reason_id, -- Coalesce(vt_LKP16.Master_Lookup_Sid,(Select Master_Lookup_Sid from vt_LKP16 where Lookup_Code='-99')) as Primary_Site_Surgery_Summary_Id,
 CAST(NULL AS INT64) AS primary_site_surgery_summary_id,
 coalesce(vt_lkp17.master_lookup_sid,
            (SELECT vt_lkp17_0.master_lookup_sid
             FROM vt_lkp17 AS vt_lkp17_0
             WHERE upper(rtrim(vt_lkp17_0.lookup_code)) = '-99' )) AS treatment_therapy_schedule_id,
 coalesce(vt_lkp18.master_lookup_sid,
            (SELECT vt_lkp18_0.master_lookup_sid
             FROM vt_lkp18 AS vt_lkp18_0
             WHERE upper(rtrim(vt_lkp18_0.lookup_code)) = '-99' )) AS definitive_radiation_modality_id,
 coalesce(vt_lkp19.master_lookup_sid,
            (SELECT vt_lkp19_0.master_lookup_sid
             FROM vt_lkp19 AS vt_lkp19_0
             WHERE upper(rtrim(vt_lkp19_0.lookup_code)) = '-99' )) AS definitive_radiation_volume_summary_id,
 coalesce(vt_lkp20.master_lookup_sid,
            (SELECT vt_lkp20_0.master_lookup_sid
             FROM vt_lkp20 AS vt_lkp20_0
             WHERE upper(rtrim(vt_lkp20_0.lookup_code)) = '-99' )) AS regional_lymph_node_summary_id,
 coalesce(vt_lkp21.master_lookup_sid,
            (SELECT vt_lkp21_0.master_lookup_sid
             FROM vt_lkp21 AS vt_lkp21_0
             WHERE upper(rtrim(vt_lkp21_0.lookup_code)) = '-99' )) AS surgery_approach_hospital_id, -- Coalesce(vt_LKP22.Master_Lookup_Sid,(Select Master_Lookup_Sid from vt_LKP22 where Lookup_Code='-99')) as Primary_Site_Surgery_Hospital_Id,
 CAST(NULL AS INT64) AS primary_site_surgery_hospital_id,
 max(stg.cs_tumsize) AS tumor_size_num_text,
 stg.mstdefchemodt AS definitive_chemo_date,
 stg.mstdefrtdt AS definitive_radiation_date,
 stg.mstdefimmunodt AS definitive_immuno_date,
 stg.mstdefhormdt AS definitive_hormone_date,
 stg.mstdefrtstopdt AS definitive_radiation_treatment_end_date,
 stg.elapsed1stsurgchem AS first_surgery_chemo_elapsed_day_num,
 stg.elapsed1stsurglstcont AS first_surgery_contact_elapsed_day_num,
 stg.elapsedchemo1stsurg AS first_chemo_surgery_elapsed_day_num,
 stg.lengthtodtchemo AS length_to_chemo_day_num,
 stg.lengthtodthorm AS length_to_hormone_day_num,
 stg.lengthtodtimmuno AS length_to_immuno_day_num,
 stg.lengthtodtmstdefsurg AS length_to_surgery_day_num,
 stg.lengthtodtradstarted AS length_to_radiation_day_num,
 stg.lengthtodttrnsplnt AS length_to_transplant_day_num,
 stg.lengthtofirsttx AS length_to_first_treatment_day_num,
 stg.dtfirstsurg AS first_surgery_date,
 stg.elapsedrtstartend AS radiation_elapsed_day_num,
 coalesce(vt_lkp23.master_lookup_sid,
            (SELECT vt_lkp23_0.master_lookup_sid
             FROM vt_lkp23 AS vt_lkp23_0
             WHERE upper(rtrim(vt_lkp23_0.lookup_code)) = '-99' )) AS tumor_size_summary_id,
 stg.dtmstdefsurg AS definitive_surgery_date, -- Will be used for CP3R C20 RECRTCT
 stg.dateadmis AS admission_date,
 stg.survival AS survival_num,
 coalesce(vt_lkp24.master_lookup_sid,
            (SELECT vt_lkp24_0.master_lookup_sid
             FROM vt_lkp24 AS vt_lkp24_0
             WHERE upper(rtrim(vt_lkp24_0.lookup_code)) = '-99' )) AS best_cs_summary_id,
 coalesce(vt_lkp25.master_lookup_sid,
            (SELECT vt_lkp25_0.master_lookup_sid
             FROM vt_lkp25 AS vt_lkp25_0
             WHERE upper(rtrim(vt_lkp25_0.lookup_code)) = '-99' )) AS best_cs_tnm_id,
 coalesce(vt_lkp26.master_lookup_sid,
            (SELECT vt_lkp26_0.master_lookup_sid
             FROM vt_lkp26 AS vt_lkp26_0
             WHERE upper(rtrim(vt_lkp26_0.lookup_code)) = '-99' )) AS definitive_treatment_facility_id,
 coalesce(vt_lkp27.master_lookup_sid,
            (SELECT vt_lkp27_0.master_lookup_sid
             FROM vt_lkp27 AS vt_lkp27_0
             WHERE upper(rtrim(vt_lkp27_0.lookup_code)) = '-99' )) AS secondary_treatment_facility_id,
 coalesce(vt_lkp28.master_lookup_sid,
            (SELECT vt_lkp28_0.master_lookup_sid
             FROM vt_lkp28 AS vt_lkp28_0
             WHERE upper(rtrim(vt_lkp28_0.lookup_code)) = '-99' )) AS tertiary_treatment_facility_id,
 max(stg.abstractor) AS abstracted_by_text,
 coalesce(vt_lkp29.master_lookup_sid,
            (SELECT vt_lkp29_0.master_lookup_sid
             FROM vt_lkp29 AS vt_lkp29_0
             WHERE upper(rtrim(vt_lkp29_0.lookup_code)) = '-99' )) AS class_case_id,
 coalesce(vt_lkp30.master_lookup_sid,
            (SELECT vt_lkp30_0.master_lookup_sid
             FROM vt_lkp30 AS vt_lkp30_0
             WHERE upper(rtrim(vt_lkp30_0.lookup_code)) = '-99' )) AS sequence_primary_id,
 max(stg.manage_md) AS managing_physician_code,
 max(stg.medoncmd) AS medical_oncology_physician_code,
 max(stg.radoncmd) AS radiation_oncology_physician_code,
 max(stg.primsurgeon) AS primary_surgeon_physician_code,
 max(stg.cs_ss_factor_1_num_code) AS cs_ss_factor_1_num_code,
 max(stg.cs_ss_factor_2_num_code) AS cs_ss_factor_2_num_code,
 max(stg.cs_ss_factor_15_num_code) AS cs_ss_factor_15_num_code,
 max(stg.cs_ss_factor_16_num_code) AS cs_ss_factor_16_num_code,
 max(stg.cs_ss_factor_22_num_code) AS cs_ss_factor_22_num_code,
 max(stg.cs_ss_factor_23_num_code) AS cs_ss_factor_23_num_code,
 max(stg.cs_ss_factor_25_num_code) AS cs_ss_factor_25_num_code,
 'M' AS source_system_code,
 datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
 stg.dtca_conf AS mltp_disciplinary_meet_1_present_date,
 coalesce(mltp1.master_lookup_sid,
            (SELECT vt_lkp31.master_lookup_sid
             FROM vt_lkp31
             WHERE upper(rtrim(vt_lkp31.lookup_code)) = '-99' )) AS mltp_disciplinary_meet_1_present_id,
 stg.dt2ca_conf AS mltp_disciplinary_meet_2_present_date,
 coalesce(mltp2.master_lookup_sid,
            (SELECT vt_lkp31.master_lookup_sid
             FROM vt_lkp31
             WHERE upper(rtrim(vt_lkp31.lookup_code)) = '-99' )) AS mltp_disciplinary_meet_2_present_id,
 stg.dt3ca_conf AS mltp_disciplinary_meet_3_present_date,
 coalesce(mltp3.master_lookup_sid,
            (SELECT vt_lkp31.master_lookup_sid
             FROM vt_lkp31
             WHERE upper(rtrim(vt_lkp31.lookup_code)) = '-99' )) AS mltp_disciplinary_meet_3_present_id
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_tumor_stg AS stg
   LEFT OUTER JOIN -- 0
 vt_lkp ON upper(rtrim(stg.grade)) = upper(rtrim(vt_lkp.lookup_code))
   LEFT OUTER JOIN -- 1
 vt_lkp1 ON upper(rtrim(stg.casestatflag)) = upper(rtrim(vt_lkp1.lookup_code))
   LEFT OUTER JOIN -- 2
 vt_lkp2 ON upper(rtrim(stg.primarysite)) = upper(rtrim(vt_lkp2.lookup_code))
   AND upper(rtrim(stg.primsitesubcode)) = upper(rtrim(vt_lkp2.lookup_sub_code))
   LEFT OUTER JOIN -- 3
 /*
        Left Outer join vt_LKP3
        ON
        STG.MstDefChemoSumm=vt_LKP3.Lookup_Code
        Left join vt_cm1
        on vt_cm1.GroupID = vt_LKP3.Lookup_Sub_Code
        AND vt_cm1.TumorID =STG.TumorID
        --4
        Left Outer join vt_LKP4
        ON
        STG.MstDefDxStageSumm=vt_LKP4.Lookup_Code
        LEFT JOIN vt_DSS
        ON vt_DSS.groupid =vt_LKP4.Lookup_Sub_Code
        AND vt_DSS.TumorID =STG.TumorID
        --5
        Left Outer join vt_LKP5
        ON
        STG.MstDefHormSumm=vt_LKP5.Lookup_Code
        Left Join vt_HS
        ON vt_HS.GroupID=vt_LKP5.Lookup_Sub_Code
        AND vt_HS.TumorID =STG.TumorID
        --6
        Left Outer join vt_LKP6
        ON
        STG.MstDefImmunoSumm=vt_LKP6.Lookup_Code
        LEFT JOIN vt_IS1
        ON vt_IS1.GroupID = vt_LKP6.Lookup_Sub_Code
        AND vt_IS1.TumorID =STG.TumorID
        */ -- 7
 vt_lkp7 ON upper(rtrim(stg.mstdefmarginssumm)) = upper(rtrim(vt_lkp7.lookup_code))
   LEFT OUTER JOIN -- 8
 /*
        Left Outer join vt_LKP8
        ON
        STG.MstDefPallSumm=vt_LKP8.Lookup_Code
        Left Outer join vt_PCS
        ON vt_PCS.GroupID = vt_LKP8.Lookup_Sub_Code
        AND vt_PCS.TumorID =STG.TumorID
        */ -- 9
 vt_lkp9 ON upper(rtrim(stg.mstdefrtboost)) = upper(rtrim(vt_lkp9.lookup_code))
   LEFT OUTER JOIN -- 10
 vt_lkp10 ON upper(rtrim(stg.mstdefrthosp)) = upper(rtrim(vt_lkp10.lookup_code))
   LEFT OUTER JOIN -- 11
 vt_lkp11 ON upper(rtrim(stg.mstdefrtloc)) = upper(rtrim(vt_lkp11.lookup_code))
   LEFT OUTER JOIN -- 12
 vt_lkp12 ON upper(rtrim(stg.reasonnochemo)) = upper(rtrim(vt_lkp12.lookup_code))
   LEFT OUTER JOIN -- 13
 vt_lkp13 ON upper(rtrim(stg.reasonnohormone)) = upper(rtrim(vt_lkp13.lookup_code))
   LEFT OUTER JOIN -- 14
 vt_lkp14 ON upper(rtrim(stg.reasonnoimmuno)) = upper(rtrim(vt_lkp14.lookup_code))
   LEFT OUTER JOIN -- 15
 vt_lkp15 ON upper(rtrim(stg.reasonnorad)) = upper(rtrim(vt_lkp15.lookup_code))
   LEFT OUTER JOIN /*
        --16
        Left Outer join vt_LKP16
        ON
        STG.MstDefSurgPrimSumm=vt_LKP16.Lookup_Code
        LEFT JOIN vt_PSSS
        ON vt_PSSS.GroupID = vt_LKP16.Lookup_Sub_Code
        AND vt_PSSS.TumorID =STG.TumorID
        */ -- 17
 vt_lkp17 ON upper(rtrim(stg.chemosrgseq)) = upper(rtrim(vt_lkp17.lookup_code))
   LEFT OUTER JOIN -- 18
 vt_lkp18 ON upper(rtrim(stg.mstdefrtmod)) = upper(rtrim(vt_lkp18.lookup_code))
   LEFT OUTER JOIN -- 19
 vt_lkp19 ON upper(rtrim(stg.mstdefrtvol)) = upper(rtrim(vt_lkp19.lookup_code))
   LEFT OUTER JOIN -- 20
 vt_lkp20 ON upper(rtrim(stg.mstdefscopelnsumm)) = upper(rtrim(vt_lkp20.lookup_code))
   LEFT OUTER JOIN -- 21
 vt_lkp21 ON upper(rtrim(stg.surgapphosp)) = upper(rtrim(vt_lkp21.lookup_code))
   LEFT OUTER JOIN -- 22
 /*
        Left Outer join vt_LKP22
        ON
        STG.MstDefSurgPrimHosp=vt_LKP22.Lookup_Code
        LEFT JOIN vt_PSSH
        ON
        vt_PSSH.GroupID = vt_LKP22.Lookup_Sub_Code
        AND vt_PSSH.TumorID =STG.TumorID
        */ -- 23
 vt_lkp23 ON upper(rtrim(stg.tumorsizesumm)) = upper(rtrim(vt_lkp23.lookup_code))
   LEFT OUTER JOIN -- 24
 vt_lkp24 ON upper(rtrim(stg.bestcssummstage)) = upper(rtrim(vt_lkp24.lookup_code))
   LEFT OUTER JOIN -- 25
 vt_lkp25 ON upper(rtrim(stg.bestcstnmstage)) = upper(rtrim(vt_lkp25.lookup_code))
   LEFT OUTER JOIN -- 26
 vt_lkp26 ON upper(rtrim(stg.t14udf116)) = upper(rtrim(vt_lkp26.lookup_code))
   LEFT OUTER JOIN -- 27
 vt_lkp27 ON upper(rtrim(stg.t14udf117)) = upper(rtrim(vt_lkp27.lookup_code))
   LEFT OUTER JOIN -- 28
 vt_lkp28 ON upper(rtrim(stg.t16udf118)) = upper(rtrim(vt_lkp28.lookup_code))
   LEFT OUTER JOIN -- 29
 vt_lkp29 ON upper(rtrim(stg.classcase)) = upper(rtrim(vt_lkp29.lookup_code))
   LEFT OUTER JOIN -- 30
 vt_lkp30 ON upper(rtrim(stg.seqprim)) = upper(rtrim(vt_lkp30.lookup_code))
   LEFT OUTER JOIN vt_lkp31 AS mltp1 ON upper(rtrim(stg.presca_conf)) = upper(rtrim(mltp1.lookup_code))
   LEFT OUTER JOIN vt_lkp31 AS mltp2 ON upper(rtrim(stg.pres2ca_conf)) = upper(rtrim(mltp2.lookup_code))
   LEFT OUTER JOIN vt_lkp31 AS mltp3 ON upper(rtrim(stg.pres3ca_conf)) = upper(rtrim(mltp3.lookup_code))
   GROUP BY 1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            16,
            17,
            18,
            19,
            20,
            21,
            22,
            23,
            24,
            25,
            upper(stg.cs_tumsize),
            27,
            28,
            29,
            30,
            31,
            32,
            33,
            34,
            35,
            36,
            37,
            38,
            39,
            40,
            41,
            42,
            43,
            44,
            45,
            46,
            47,
            48,
            49,
            50,
            51,
            52,
            upper(stg.abstractor),
            54,
            55,
            upper(stg.manage_md),
            upper(stg.medoncmd),
            upper(stg.radoncmd),
            upper(stg.primsurgeon),
            upper(stg.cs_ss_factor_1_num_code),
            upper(stg.cs_ss_factor_2_num_code),
            upper(stg.cs_ss_factor_15_num_code),
            upper(stg.cs_ss_factor_16_num_code),
            upper(stg.cs_ss_factor_22_num_code),
            upper(stg.cs_ss_factor_23_num_code),
            upper(stg.cs_ss_factor_25_num_code),
            68,
            69,
            70,
            71,
            72,
            73,
            74) AS ms ON mt.cr_patient_id = ms.cr_patient_id
AND mt.tumor_id = ms.tumor_id
AND (coalesce(mt.tumor_grade_id, 0) = coalesce(ms.tumor_grade_id, 0)
     AND coalesce(mt.tumor_grade_id, 1) = coalesce(ms.tumor_grade_id, 1))
AND (coalesce(mt.case_status_id, 0) = coalesce(ms.case_status_id, 0)
     AND coalesce(mt.case_status_id, 1) = coalesce(ms.case_status_id, 1))
AND (coalesce(mt.tumor_primary_site_id, 0) = coalesce(ms.tumor_primary_site_id, 0)
     AND coalesce(mt.tumor_primary_site_id, 1) = coalesce(ms.tumor_primary_site_id, 1))
AND (coalesce(mt.definitive_chemo_summary_id, 0) = coalesce(ms.definitive_chemo_summary_id, 0)
     AND coalesce(mt.definitive_chemo_summary_id, 1) = coalesce(ms.definitive_chemo_summary_id, 1))
AND (coalesce(mt.definitive_diagnostic_stage_summary_id, 0) = coalesce(ms.definitive_diagnostic_stage_summary_id, 0)
     AND coalesce(mt.definitive_diagnostic_stage_summary_id, 1) = coalesce(ms.definitive_diagnostic_stage_summary_id, 1))
AND (coalesce(mt.definitive_hormone_summary_id, 0) = coalesce(ms.definitive_hormone_summary_id, 0)
     AND coalesce(mt.definitive_hormone_summary_id, 1) = coalesce(ms.definitive_hormone_summary_id, 1))
AND (coalesce(mt.definitive_immuno_summary_id, 0) = coalesce(ms.definitive_immuno_summary_id, 0)
     AND coalesce(mt.definitive_immuno_summary_id, 1) = coalesce(ms.definitive_immuno_summary_id, 1))
AND (coalesce(mt.definitive_surgical_margin_summary_id, 0) = coalesce(ms.definitive_surgical_margin_summary_id, 0)
     AND coalesce(mt.definitive_surgical_margin_summary_id, 1) = coalesce(ms.definitive_surgical_margin_summary_id, 1))
AND (coalesce(mt.definitive_palliative_care_summary_id, 0) = coalesce(ms.definitive_palliative_care_summary_id, 0)
     AND coalesce(mt.definitive_palliative_care_summary_id, 1) = coalesce(ms.definitive_palliative_care_summary_id, 1))
AND (coalesce(mt.radiation_boost_modality_id, 0) = coalesce(ms.radiation_boost_modality_id, 0)
     AND coalesce(mt.radiation_boost_modality_id, 1) = coalesce(ms.radiation_boost_modality_id, 1))
AND (coalesce(mt.radiation_hospital_id, 0) = coalesce(ms.radiation_hospital_id, 0)
     AND coalesce(mt.radiation_hospital_id, 1) = coalesce(ms.radiation_hospital_id, 1))
AND (coalesce(mt.radiation_site_id, 0) = coalesce(ms.radiation_site_id, 0)
     AND coalesce(mt.radiation_site_id, 1) = coalesce(ms.radiation_site_id, 1))
AND (coalesce(mt.chemo_declined_reason_id, 0) = coalesce(ms.chemo_declined_reason_id, 0)
     AND coalesce(mt.chemo_declined_reason_id, 1) = coalesce(ms.chemo_declined_reason_id, 1))
AND (coalesce(mt.hormone_declined_reason_id, 0) = coalesce(ms.hormone_declined_reason_id, 0)
     AND coalesce(mt.hormone_declined_reason_id, 1) = coalesce(ms.hormone_declined_reason_id, 1))
AND (coalesce(mt.immuno_declined_reason_id, 0) = coalesce(ms.immuno_declined_reason_id, 0)
     AND coalesce(mt.immuno_declined_reason_id, 1) = coalesce(ms.immuno_declined_reason_id, 1))
AND (coalesce(mt.radiation_declined_reason_id, 0) = coalesce(ms.radiation_declined_reason_id, 0)
     AND coalesce(mt.radiation_declined_reason_id, 1) = coalesce(ms.radiation_declined_reason_id, 1))
AND (coalesce(mt.primary_site_surgery_summary_id, 0) = coalesce(ms.primary_site_surgery_summary_id, 0)
     AND coalesce(mt.primary_site_surgery_summary_id, 1) = coalesce(ms.primary_site_surgery_summary_id, 1))
AND (coalesce(mt.treatment_therapy_schedule_id, 0) = coalesce(ms.treatment_therapy_schedule_id, 0)
     AND coalesce(mt.treatment_therapy_schedule_id, 1) = coalesce(ms.treatment_therapy_schedule_id, 1))
AND (coalesce(mt.definitive_radiation_modality_id, 0) = coalesce(ms.definitive_radiation_modality_id, 0)
     AND coalesce(mt.definitive_radiation_modality_id, 1) = coalesce(ms.definitive_radiation_modality_id, 1))
AND (coalesce(mt.definitive_radiation_volume_summary_id, 0) = coalesce(ms.definitive_radiation_volume_summary_id, 0)
     AND coalesce(mt.definitive_radiation_volume_summary_id, 1) = coalesce(ms.definitive_radiation_volume_summary_id, 1))
AND (coalesce(mt.regional_lymph_node_summary_id, 0) = coalesce(ms.regional_lymph_node_summary_id, 0)
     AND coalesce(mt.regional_lymph_node_summary_id, 1) = coalesce(ms.regional_lymph_node_summary_id, 1))
AND (coalesce(mt.surgery_approach_hospital_id, 0) = coalesce(ms.surgery_approach_hospital_id, 0)
     AND coalesce(mt.surgery_approach_hospital_id, 1) = coalesce(ms.surgery_approach_hospital_id, 1))
AND (coalesce(mt.primary_site_surgery_hospital_id, 0) = coalesce(ms.primary_site_surgery_hospital_id, 0)
     AND coalesce(mt.primary_site_surgery_hospital_id, 1) = coalesce(ms.primary_site_surgery_hospital_id, 1))
AND (upper(coalesce(mt.tumor_size_num_text, '0')) = upper(coalesce(ms.tumor_size_num_text, '0'))
     AND upper(coalesce(mt.tumor_size_num_text, '1')) = upper(coalesce(ms.tumor_size_num_text, '1')))
AND (coalesce(mt.definitive_chemo_date, DATE '1970-01-01') = coalesce(ms.definitive_chemo_date, DATE '1970-01-01')
     AND coalesce(mt.definitive_chemo_date, DATE '1970-01-02') = coalesce(ms.definitive_chemo_date, DATE '1970-01-02'))
AND (coalesce(mt.definitive_radiation_date, DATE '1970-01-01') = coalesce(ms.definitive_radiation_date, DATE '1970-01-01')
     AND coalesce(mt.definitive_radiation_date, DATE '1970-01-02') = coalesce(ms.definitive_radiation_date, DATE '1970-01-02'))
AND (coalesce(mt.definitive_immuno_date, DATE '1970-01-01') = coalesce(ms.definitive_immuno_date, DATE '1970-01-01')
     AND coalesce(mt.definitive_immuno_date, DATE '1970-01-02') = coalesce(ms.definitive_immuno_date, DATE '1970-01-02'))
AND (coalesce(mt.definitive_hormone_date, DATE '1970-01-01') = coalesce(ms.definitive_hormone_date, DATE '1970-01-01')
     AND coalesce(mt.definitive_hormone_date, DATE '1970-01-02') = coalesce(ms.definitive_hormone_date, DATE '1970-01-02'))
AND (coalesce(mt.definitive_radiation_treatment_end_date, DATE '1970-01-01') = coalesce(ms.definitive_radiation_treatment_end_date, DATE '1970-01-01')
     AND coalesce(mt.definitive_radiation_treatment_end_date, DATE '1970-01-02') = coalesce(ms.definitive_radiation_treatment_end_date, DATE '1970-01-02'))
AND (coalesce(mt.first_surgery_chemo_elapsed_day_num, 0) = coalesce(ms.first_surgery_chemo_elapsed_day_num, 0)
     AND coalesce(mt.first_surgery_chemo_elapsed_day_num, 1) = coalesce(ms.first_surgery_chemo_elapsed_day_num, 1))
AND (coalesce(mt.first_surgery_contact_elapsed_day_num, 0) = coalesce(ms.first_surgery_contact_elapsed_day_num, 0)
     AND coalesce(mt.first_surgery_contact_elapsed_day_num, 1) = coalesce(ms.first_surgery_contact_elapsed_day_num, 1))
AND (coalesce(mt.first_chemo_surgery_elapsed_day_num, 0) = coalesce(ms.first_chemo_surgery_elapsed_day_num, 0)
     AND coalesce(mt.first_chemo_surgery_elapsed_day_num, 1) = coalesce(ms.first_chemo_surgery_elapsed_day_num, 1))
AND (coalesce(mt.length_to_chemo_day_num, 0) = coalesce(ms.length_to_chemo_day_num, 0)
     AND coalesce(mt.length_to_chemo_day_num, 1) = coalesce(ms.length_to_chemo_day_num, 1))
AND (coalesce(mt.length_to_hormone_day_num, 0) = coalesce(ms.length_to_hormone_day_num, 0)
     AND coalesce(mt.length_to_hormone_day_num, 1) = coalesce(ms.length_to_hormone_day_num, 1))
AND (coalesce(mt.length_to_immuno_day_num, 0) = coalesce(ms.length_to_immuno_day_num, 0)
     AND coalesce(mt.length_to_immuno_day_num, 1) = coalesce(ms.length_to_immuno_day_num, 1))
AND (coalesce(mt.length_to_surgery_day_num, 0) = coalesce(ms.length_to_surgery_day_num, 0)
     AND coalesce(mt.length_to_surgery_day_num, 1) = coalesce(ms.length_to_surgery_day_num, 1))
AND (coalesce(mt.length_to_radiation_day_num, 0) = coalesce(ms.length_to_radiation_day_num, 0)
     AND coalesce(mt.length_to_radiation_day_num, 1) = coalesce(ms.length_to_radiation_day_num, 1))
AND (coalesce(mt.length_to_transplant_day_num, 0) = coalesce(ms.length_to_transplant_day_num, 0)
     AND coalesce(mt.length_to_transplant_day_num, 1) = coalesce(ms.length_to_transplant_day_num, 1))
AND (coalesce(mt.length_to_first_treatment_day_num, 0) = coalesce(ms.length_to_first_treatment_day_num, 0)
     AND coalesce(mt.length_to_first_treatment_day_num, 1) = coalesce(ms.length_to_first_treatment_day_num, 1))
AND (coalesce(mt.first_surgery_date, DATE '1970-01-01') = coalesce(ms.first_surgery_date, DATE '1970-01-01')
     AND coalesce(mt.first_surgery_date, DATE '1970-01-02') = coalesce(ms.first_surgery_date, DATE '1970-01-02'))
AND (coalesce(mt.radiation_elapsed_day_num, 0) = coalesce(ms.radiation_elapsed_day_num, 0)
     AND coalesce(mt.radiation_elapsed_day_num, 1) = coalesce(ms.radiation_elapsed_day_num, 1))
AND (coalesce(mt.tumor_size_summary_id, 0) = coalesce(ms.tumor_size_summary_id, 0)
     AND coalesce(mt.tumor_size_summary_id, 1) = coalesce(ms.tumor_size_summary_id, 1))
AND (coalesce(mt.definitive_surgery_date, DATE '1970-01-01') = coalesce(ms.definitive_surgery_date, DATE '1970-01-01')
     AND coalesce(mt.definitive_surgery_date, DATE '1970-01-02') = coalesce(ms.definitive_surgery_date, DATE '1970-01-02'))
AND (coalesce(mt.admission_date, DATE '1970-01-01') = coalesce(ms.admission_date, DATE '1970-01-01')
     AND coalesce(mt.admission_date, DATE '1970-01-02') = coalesce(ms.admission_date, DATE '1970-01-02'))
AND (coalesce(mt.survival_num, 0) = coalesce(ms.survival_num, 0)
     AND coalesce(mt.survival_num, 1) = coalesce(ms.survival_num, 1))
AND (coalesce(mt.best_cs_summary_id, 0) = coalesce(ms.best_cs_summary_id, 0)
     AND coalesce(mt.best_cs_summary_id, 1) = coalesce(ms.best_cs_summary_id, 1))
AND (coalesce(mt.best_cs_tnm_id, 0) = coalesce(ms.best_cs_tnm_id, 0)
     AND coalesce(mt.best_cs_tnm_id, 1) = coalesce(ms.best_cs_tnm_id, 1))
AND (coalesce(mt.definitive_treatment_facility_id, 0) = coalesce(ms.definitive_treatment_facility_id, 0)
     AND coalesce(mt.definitive_treatment_facility_id, 1) = coalesce(ms.definitive_treatment_facility_id, 1))
AND (coalesce(mt.secondary_treatment_facility_id, 0) = coalesce(ms.secondary_treatment_facility_id, 0)
     AND coalesce(mt.secondary_treatment_facility_id, 1) = coalesce(ms.secondary_treatment_facility_id, 1))
AND (coalesce(mt.tertiary_treatment_facility_id, 0) = coalesce(ms.tertiary_treatment_facility_id, 0)
     AND coalesce(mt.tertiary_treatment_facility_id, 1) = coalesce(ms.tertiary_treatment_facility_id, 1))
AND (upper(coalesce(mt.abstracted_by_text, '0')) = upper(coalesce(ms.abstracted_by_text, '0'))
     AND upper(coalesce(mt.abstracted_by_text, '1')) = upper(coalesce(ms.abstracted_by_text, '1')))
AND (coalesce(mt.class_case_id, 0) = coalesce(ms.class_case_id, 0)
     AND coalesce(mt.class_case_id, 1) = coalesce(ms.class_case_id, 1))
AND (coalesce(mt.sequence_primary_id, 0) = coalesce(ms.sequence_primary_id, 0)
     AND coalesce(mt.sequence_primary_id, 1) = coalesce(ms.sequence_primary_id, 1))
AND (upper(coalesce(mt.managing_physician_code, '0')) = upper(coalesce(ms.managing_physician_code, '0'))
     AND upper(coalesce(mt.managing_physician_code, '1')) = upper(coalesce(ms.managing_physician_code, '1')))
AND (upper(coalesce(mt.medical_oncology_physician_code, '0')) = upper(coalesce(ms.medical_oncology_physician_code, '0'))
     AND upper(coalesce(mt.medical_oncology_physician_code, '1')) = upper(coalesce(ms.medical_oncology_physician_code, '1')))
AND (upper(coalesce(mt.radiation_oncology_physician_code, '0')) = upper(coalesce(ms.radiation_oncology_physician_code, '0'))
     AND upper(coalesce(mt.radiation_oncology_physician_code, '1')) = upper(coalesce(ms.radiation_oncology_physician_code, '1')))
AND (upper(coalesce(mt.primary_surgeon_physician_code, '0')) = upper(coalesce(ms.primary_surgeon_physician_code, '0'))
     AND upper(coalesce(mt.primary_surgeon_physician_code, '1')) = upper(coalesce(ms.primary_surgeon_physician_code, '1')))
AND (upper(coalesce(mt.cs_ss_factor_1_num_code, '0')) = upper(coalesce(ms.cs_ss_factor_1_num_code, '0'))
     AND upper(coalesce(mt.cs_ss_factor_1_num_code, '1')) = upper(coalesce(ms.cs_ss_factor_1_num_code, '1')))
AND (upper(coalesce(mt.cs_ss_factor_2_num_code, '0')) = upper(coalesce(ms.cs_ss_factor_2_num_code, '0'))
     AND upper(coalesce(mt.cs_ss_factor_2_num_code, '1')) = upper(coalesce(ms.cs_ss_factor_2_num_code, '1')))
AND (upper(coalesce(mt.cs_ss_factor_15_num_code, '0')) = upper(coalesce(ms.cs_ss_factor_15_num_code, '0'))
     AND upper(coalesce(mt.cs_ss_factor_15_num_code, '1')) = upper(coalesce(ms.cs_ss_factor_15_num_code, '1')))
AND (upper(coalesce(mt.cs_ss_factor_16_num_code, '0')) = upper(coalesce(ms.cs_ss_factor_16_num_code, '0'))
     AND upper(coalesce(mt.cs_ss_factor_16_num_code, '1')) = upper(coalesce(ms.cs_ss_factor_16_num_code, '1')))
AND (upper(coalesce(mt.cs_ss_factor_22_num_code, '0')) = upper(coalesce(ms.cs_ss_factor_22_num_code, '0'))
     AND upper(coalesce(mt.cs_ss_factor_22_num_code, '1')) = upper(coalesce(ms.cs_ss_factor_22_num_code, '1')))
AND (upper(coalesce(mt.cs_ss_factor_23_num_code, '0')) = upper(coalesce(ms.cs_ss_factor_23_num_code, '0'))
     AND upper(coalesce(mt.cs_ss_factor_23_num_code, '1')) = upper(coalesce(ms.cs_ss_factor_23_num_code, '1')))
AND (upper(coalesce(mt.cs_ss_factor_25_num_code, '0')) = upper(coalesce(ms.cs_ss_factor_25_num_code, '0'))
     AND upper(coalesce(mt.cs_ss_factor_25_num_code, '1')) = upper(coalesce(ms.cs_ss_factor_25_num_code, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time
AND (coalesce(mt.mltp_disciplinary_meet_1_present_date, DATE '1970-01-01') = coalesce(ms.mltp_disciplinary_meet_1_present_date, DATE '1970-01-01')
     AND coalesce(mt.mltp_disciplinary_meet_1_present_date, DATE '1970-01-02') = coalesce(ms.mltp_disciplinary_meet_1_present_date, DATE '1970-01-02'))
AND (coalesce(mt.mltp_disciplinary_meet_1_present_id, 0) = coalesce(ms.mltp_disciplinary_meet_1_present_id, 0)
     AND coalesce(mt.mltp_disciplinary_meet_1_present_id, 1) = coalesce(ms.mltp_disciplinary_meet_1_present_id, 1))
AND (coalesce(mt.mltp_disciplinary_meet_2_present_date, DATE '1970-01-01') = coalesce(ms.mltp_disciplinary_meet_2_present_date, DATE '1970-01-01')
     AND coalesce(mt.mltp_disciplinary_meet_2_present_date, DATE '1970-01-02') = coalesce(ms.mltp_disciplinary_meet_2_present_date, DATE '1970-01-02'))
AND (coalesce(mt.mltp_disciplinary_meet_2_present_id, 0) = coalesce(ms.mltp_disciplinary_meet_2_present_id, 0)
     AND coalesce(mt.mltp_disciplinary_meet_2_present_id, 1) = coalesce(ms.mltp_disciplinary_meet_2_present_id, 1))
AND (coalesce(mt.mltp_disciplinary_meet_3_present_date, DATE '1970-01-01') = coalesce(ms.mltp_disciplinary_meet_3_present_date, DATE '1970-01-01')
     AND coalesce(mt.mltp_disciplinary_meet_3_present_date, DATE '1970-01-02') = coalesce(ms.mltp_disciplinary_meet_3_present_date, DATE '1970-01-02'))
AND (coalesce(mt.mltp_disciplinary_meet_3_present_id, 0) = coalesce(ms.mltp_disciplinary_meet_3_present_id, 0)
     AND coalesce(mt.mltp_disciplinary_meet_3_present_id, 1) = coalesce(ms.mltp_disciplinary_meet_3_present_id, 1)) WHEN NOT MATCHED BY TARGET THEN
INSERT (cr_patient_id,
        tumor_id,
        tumor_grade_id,
        case_status_id,
        tumor_primary_site_id,
        definitive_chemo_summary_id,
        definitive_diagnostic_stage_summary_id,
        definitive_hormone_summary_id,
        definitive_immuno_summary_id,
        definitive_surgical_margin_summary_id,
        definitive_palliative_care_summary_id,
        radiation_boost_modality_id,
        radiation_hospital_id,
        radiation_site_id,
        chemo_declined_reason_id,
        hormone_declined_reason_id,
        immuno_declined_reason_id,
        radiation_declined_reason_id,
        primary_site_surgery_summary_id,
        treatment_therapy_schedule_id,
        definitive_radiation_modality_id,
        definitive_radiation_volume_summary_id,
        regional_lymph_node_summary_id,
        surgery_approach_hospital_id,
        primary_site_surgery_hospital_id,
        tumor_size_num_text,
        definitive_chemo_date,
        definitive_radiation_date,
        definitive_immuno_date,
        definitive_hormone_date,
        definitive_radiation_treatment_end_date,
        first_surgery_chemo_elapsed_day_num,
        first_surgery_contact_elapsed_day_num,
        first_chemo_surgery_elapsed_day_num,
        length_to_chemo_day_num,
        length_to_hormone_day_num,
        length_to_immuno_day_num,
        length_to_surgery_day_num,
        length_to_radiation_day_num,
        length_to_transplant_day_num,
        length_to_first_treatment_day_num,
        first_surgery_date,
        radiation_elapsed_day_num,
        tumor_size_summary_id,
        definitive_surgery_date,
        admission_date,
        survival_num,
        best_cs_summary_id,
        best_cs_tnm_id,
        definitive_treatment_facility_id,
        secondary_treatment_facility_id,
        tertiary_treatment_facility_id,
        abstracted_by_text,
        class_case_id,
        sequence_primary_id,
        managing_physician_code,
        medical_oncology_physician_code,
        radiation_oncology_physician_code,
        primary_surgeon_physician_code,
        cs_ss_factor_1_num_code,
        cs_ss_factor_2_num_code,
        cs_ss_factor_15_num_code,
        cs_ss_factor_16_num_code,
        cs_ss_factor_22_num_code,
        cs_ss_factor_23_num_code,
        cs_ss_factor_25_num_code,
        source_system_code,
        dw_last_update_date_time,
        mltp_disciplinary_meet_1_present_date,
        mltp_disciplinary_meet_1_present_id,
        mltp_disciplinary_meet_2_present_date,
        mltp_disciplinary_meet_2_present_id,
        mltp_disciplinary_meet_3_present_date,
        mltp_disciplinary_meet_3_present_id)
VALUES (ms.cr_patient_id, ms.tumor_id, ms.tumor_grade_id, ms.case_status_id, ms.tumor_primary_site_id, ms.definitive_chemo_summary_id, ms.definitive_diagnostic_stage_summary_id, ms.definitive_hormone_summary_id, ms.definitive_immuno_summary_id, ms.definitive_surgical_margin_summary_id, ms.definitive_palliative_care_summary_id, ms.radiation_boost_modality_id, ms.radiation_hospital_id, ms.radiation_site_id, ms.chemo_declined_reason_id, ms.hormone_declined_reason_id, ms.immuno_declined_reason_id, ms.radiation_declined_reason_id, ms.primary_site_surgery_summary_id, ms.treatment_therapy_schedule_id, ms.definitive_radiation_modality_id, ms.definitive_radiation_volume_summary_id, ms.regional_lymph_node_summary_id, ms.surgery_approach_hospital_id, ms.primary_site_surgery_hospital_id, ms.tumor_size_num_text, ms.definitive_chemo_date, ms.definitive_radiation_date, ms.definitive_immuno_date, ms.definitive_hormone_date, ms.definitive_radiation_treatment_end_date, ms.first_surgery_chemo_elapsed_day_num, ms.first_surgery_contact_elapsed_day_num, ms.first_chemo_surgery_elapsed_day_num, ms.length_to_chemo_day_num, ms.length_to_hormone_day_num, ms.length_to_immuno_day_num, ms.length_to_surgery_day_num, ms.length_to_radiation_day_num, ms.length_to_transplant_day_num, ms.length_to_first_treatment_day_num, ms.first_surgery_date, ms.radiation_elapsed_day_num, ms.tumor_size_summary_id, ms.definitive_surgery_date, ms.admission_date, ms.survival_num, ms.best_cs_summary_id, ms.best_cs_tnm_id, ms.definitive_treatment_facility_id, ms.secondary_treatment_facility_id, ms.tertiary_treatment_facility_id, ms.abstracted_by_text, ms.class_case_id, ms.sequence_primary_id, ms.managing_physician_code, ms.medical_oncology_physician_code, ms.radiation_oncology_physician_code, ms.primary_surgeon_physician_code, ms.cs_ss_factor_1_num_code, ms.cs_ss_factor_2_num_code, ms.cs_ss_factor_15_num_code, ms.cs_ss_factor_16_num_code, ms.cs_ss_factor_22_num_code, ms.cs_ss_factor_23_num_code, ms.cs_ss_factor_25_num_code, ms.source_system_code, ms.dw_last_update_date_time, ms.mltp_disciplinary_meet_1_present_date, ms.mltp_disciplinary_meet_1_present_id, ms.mltp_disciplinary_meet_2_present_date, ms.mltp_disciplinary_meet_2_present_id, ms.mltp_disciplinary_meet_3_present_date, ms.mltp_disciplinary_meet_3_present_id);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT tumor_id
      FROM `hca-hin-dev-cur-ops`.edwcr.cr_patient_tumor
      GROUP BY tumor_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.cr_patient_tumor');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_lkp;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_lkp1;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_lkp2;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_lkp7;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_lkp9;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_lkp10;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_lkp11;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_lkp12;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_lkp13;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_lkp14;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_lkp15;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_lkp17;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_lkp18;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_lkp19;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_lkp20;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_lkp21;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_lkp23;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_lkp24;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_lkp25;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_lkp26;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_lkp27;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_lkp28;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_lkp29;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_lkp30;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_lkp31;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_cm1 CLUSTER BY groupid,
                                  tumorid AS
SELECT max(t3.group_id) AS groupid,
       t3.lookup_id,
       t1.tumorid
FROM `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_tumor_stg AS t1
INNER JOIN `hca-hin-dev-cur-ops`.edwcr_staging.ref_lookup_groups_stg AS t3 ON t3.beginhisto <= CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(left(t1.histology, 4)) AS FLOAT64)
AND t3.endhisto >= CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(left(t1.histology, 4)) AS FLOAT64)
AND t3.beginprimarysite <= CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(CASE
                                                                                            WHEN upper(rtrim(right(t1.primarysite, 3))) = '***' THEN '000'
                                                                                            ELSE right(t1.primarysite, 3)
                                                                                        END) AS FLOAT64)
AND t3.endprimarysite >= CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(CASE
                                                                                          WHEN upper(rtrim(right(t1.primarysite, 3))) = '***' THEN '000'
                                                                                          ELSE right(t1.primarysite, 3)
                                                                                      END) AS FLOAT64)
AND t3.beginrxtype <= 4
AND t3.endrxtype >= 4
AND t3.lookup_id = 4043
GROUP BY 2,
         3;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


UPDATE `hca-hin-dev-cur-ops`.edwcr.cr_patient_tumor AS tu
SET definitive_chemo_summary_id = vt_lkp3.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_tumor_stg AS stg
CROSS JOIN vt_lkp3
CROSS JOIN vt_cm1
WHERE tu.tumor_id = stg.tumorid
  AND upper(rtrim(stg.mstdefchemosumm)) = upper(rtrim(vt_lkp3.lookup_code))
  AND vt_cm1.groupid = CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(vt_lkp3.lookup_sub_code) AS FLOAT64)
  AND vt_cm1.tumorid = stg.tumorid;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


UPDATE `hca-hin-dev-cur-ops`.edwcr.cr_patient_tumor AS tu
SET definitive_chemo_summary_id =
  (SELECT vt_lkp3.master_lookup_sid
   FROM vt_lkp3
   WHERE upper(rtrim(vt_lkp3.lookup_code)) = '-99' )
WHERE tu.definitive_chemo_summary_id IS NULL;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_cm1;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_lkp3;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_dss CLUSTER BY groupid,
                                  tumorid AS
SELECT max(t3.group_id) AS groupid,
       t3.lookup_id,
       t1.tumorid
FROM `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_tumor_stg AS t1
INNER JOIN `hca-hin-dev-cur-ops`.edwcr_staging.ref_lookup_groups_stg AS t3 ON t3.beginhisto <= CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(left(t1.histology, 4)) AS FLOAT64)
AND t3.endhisto >= CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(left(t1.histology, 4)) AS FLOAT64)
AND t3.beginprimarysite <= CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(CASE
                                                                                            WHEN upper(rtrim(right(t1.primarysite, 3))) = '***' THEN '000'
                                                                                            ELSE right(t1.primarysite, 3)
                                                                                        END) AS FLOAT64)
AND t3.endprimarysite >= CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(CASE
                                                                                          WHEN upper(rtrim(right(t1.primarysite, 3))) = '***' THEN '000'
                                                                                          ELSE right(t1.primarysite, 3)
                                                                                      END) AS FLOAT64)
AND t3.beginrxtype <= 0
AND t3.endrxtype >= 0
AND t3.lookup_id = 4043
GROUP BY 2,
         3;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


UPDATE `hca-hin-dev-cur-ops`.edwcr.cr_patient_tumor AS tu
SET definitive_diagnostic_stage_summary_id = vt_lkp4.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_tumor_stg AS stg
CROSS JOIN vt_lkp4
CROSS JOIN vt_dss
WHERE tu.tumor_id = stg.tumorid
  AND upper(rtrim(stg.mstdefdxstagesumm)) = upper(rtrim(vt_lkp4.lookup_code))
  AND vt_dss.groupid = CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(vt_lkp4.lookup_sub_code) AS FLOAT64)
  AND vt_dss.tumorid = stg.tumorid;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


UPDATE `hca-hin-dev-cur-ops`.edwcr.cr_patient_tumor AS tu
SET definitive_diagnostic_stage_summary_id =
  (SELECT vt_lkp4.master_lookup_sid
   FROM vt_lkp4
   WHERE upper(rtrim(vt_lkp4.lookup_code)) = '-99' )
WHERE tu.definitive_diagnostic_stage_summary_id IS NULL;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_dss;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_lkp4;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_hs CLUSTER BY groupid,
                                 tumorid AS
SELECT max(t3.group_id) AS groupid,
       t3.lookup_id,
       t1.tumorid
FROM `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_tumor_stg AS t1
INNER JOIN `hca-hin-dev-cur-ops`.edwcr_staging.ref_lookup_groups_stg AS t3 ON t3.beginhisto <= CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(left(t1.histology, 4)) AS FLOAT64)
AND t3.endhisto >= CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(left(t1.histology, 4)) AS FLOAT64)
AND t3.beginprimarysite <= CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(CASE
                                                                                            WHEN upper(rtrim(right(t1.primarysite, 3))) = '***' THEN '000'
                                                                                            ELSE right(t1.primarysite, 3)
                                                                                        END) AS FLOAT64)
AND t3.endprimarysite >= CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(CASE
                                                                                          WHEN upper(rtrim(right(t1.primarysite, 3))) = '***' THEN '000'
                                                                                          ELSE right(t1.primarysite, 3)
                                                                                      END) AS FLOAT64)
AND t3.beginrxtype <= 5
AND t3.endrxtype >= 5
AND t3.lookup_id = 4043
GROUP BY 2,
         3;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


UPDATE `hca-hin-dev-cur-ops`.edwcr.cr_patient_tumor AS tu
SET definitive_hormone_summary_id = vt_lkp5.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_tumor_stg AS stg
CROSS JOIN vt_lkp5
CROSS JOIN vt_hs
WHERE tu.tumor_id = stg.tumorid
  AND upper(rtrim(stg.mstdefhormsumm)) = upper(rtrim(vt_lkp5.lookup_code))
  AND vt_hs.groupid = CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(vt_lkp5.lookup_sub_code) AS FLOAT64)
  AND vt_hs.tumorid = stg.tumorid;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


UPDATE `hca-hin-dev-cur-ops`.edwcr.cr_patient_tumor AS tu
SET definitive_hormone_summary_id =
  (SELECT vt_lkp5.master_lookup_sid
   FROM vt_lkp5
   WHERE upper(rtrim(vt_lkp5.lookup_code)) = '-99' )
WHERE tu.definitive_hormone_summary_id IS NULL;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_hs;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_lkp5;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_is1 CLUSTER BY groupid,
                                  tumorid AS
SELECT max(t3.group_id) AS groupid,
       t3.lookup_id,
       t1.tumorid
FROM `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_tumor_stg AS t1
INNER JOIN `hca-hin-dev-cur-ops`.edwcr_staging.ref_lookup_groups_stg AS t3 ON t3.beginhisto <= CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(left(t1.histology, 4)) AS FLOAT64)
AND t3.endhisto >= CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(left(t1.histology, 4)) AS FLOAT64)
AND t3.beginprimarysite <= CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(CASE
                                                                                            WHEN upper(rtrim(right(t1.primarysite, 3))) = '***' THEN '000'
                                                                                            ELSE right(t1.primarysite, 3)
                                                                                        END) AS FLOAT64)
AND t3.endprimarysite >= CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(CASE
                                                                                          WHEN upper(rtrim(right(t1.primarysite, 3))) = '***' THEN '000'
                                                                                          ELSE right(t1.primarysite, 3)
                                                                                      END) AS FLOAT64)
AND t3.beginrxtype <= 6
AND t3.endrxtype >= 6
AND t3.lookup_id = 4043
GROUP BY 2,
         3;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


UPDATE `hca-hin-dev-cur-ops`.edwcr.cr_patient_tumor AS tu
SET definitive_immuno_summary_id = vt_lkp6.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_tumor_stg AS stg
CROSS JOIN vt_lkp6
CROSS JOIN vt_is1
WHERE tu.tumor_id = stg.tumorid
  AND upper(rtrim(stg.mstdefimmunosumm)) = upper(rtrim(vt_lkp6.lookup_code))
  AND vt_is1.groupid = CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(vt_lkp6.lookup_sub_code) AS FLOAT64)
  AND vt_is1.tumorid = stg.tumorid;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


UPDATE `hca-hin-dev-cur-ops`.edwcr.cr_patient_tumor AS tu
SET definitive_immuno_summary_id =
  (SELECT vt_lkp6.master_lookup_sid
   FROM vt_lkp6
   WHERE upper(rtrim(vt_lkp6.lookup_code)) = '-99' )
WHERE tu.definitive_immuno_summary_id IS NULL;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_is1;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_lkp6;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_pcs CLUSTER BY groupid,
                                  tumorid AS
SELECT max(t3.group_id) AS groupid,
       t3.lookup_id,
       t1.tumorid
FROM `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_tumor_stg AS t1
INNER JOIN `hca-hin-dev-cur-ops`.edwcr_staging.ref_lookup_groups_stg AS t3 ON t3.beginhisto <= CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(left(t1.histology, 4)) AS FLOAT64)
AND t3.endhisto >= CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(left(t1.histology, 4)) AS FLOAT64)
AND t3.beginprimarysite <= CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(CASE
                                                                                            WHEN upper(rtrim(right(t1.primarysite, 3))) = '***' THEN '000'
                                                                                            ELSE right(t1.primarysite, 3)
                                                                                        END) AS FLOAT64)
AND t3.endprimarysite >= CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(CASE
                                                                                          WHEN upper(rtrim(right(t1.primarysite, 3))) = '***' THEN '000'
                                                                                          ELSE right(t1.primarysite, 3)
                                                                                      END) AS FLOAT64)
AND t3.beginrxtype <= 1
AND t3.endrxtype >= 1
AND t3.lookup_id = 4043
GROUP BY 2,
         3;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


UPDATE `hca-hin-dev-cur-ops`.edwcr.cr_patient_tumor AS tu
SET definitive_palliative_care_summary_id = vt_lkp8.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_tumor_stg AS stg
CROSS JOIN vt_lkp8
CROSS JOIN vt_pcs
WHERE tu.tumor_id = stg.tumorid
  AND upper(rtrim(stg.mstdefpallsumm)) = upper(rtrim(vt_lkp8.lookup_code))
  AND vt_pcs.groupid = CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(vt_lkp8.lookup_sub_code) AS FLOAT64)
  AND vt_pcs.tumorid = stg.tumorid;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


UPDATE `hca-hin-dev-cur-ops`.edwcr.cr_patient_tumor AS tu
SET definitive_palliative_care_summary_id =
  (SELECT vt_lkp8.master_lookup_sid
   FROM vt_lkp8
   WHERE upper(rtrim(vt_lkp8.lookup_code)) = '-99' )
WHERE tu.definitive_palliative_care_summary_id IS NULL;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_pcs;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_lkp8;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_psss CLUSTER BY groupid,
                                   tumorid AS
SELECT max(t3.group_id) AS groupid,
       t3.lookup_id,
       t1.tumorid
FROM `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_tumor_stg AS t1
INNER JOIN `hca-hin-dev-cur-ops`.edwcr_staging.ref_lookup_groups_stg AS t3 ON t3.beginhisto <= CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(left(t1.histology, 4)) AS FLOAT64)
AND t3.endhisto >= CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(left(t1.histology, 4)) AS FLOAT64)
AND t3.beginprimarysite <= CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(CASE
                                                                                            WHEN upper(rtrim(right(t1.primarysite, 3))) = '***' THEN '000'
                                                                                            ELSE right(t1.primarysite, 3)
                                                                                        END) AS FLOAT64)
AND t3.endprimarysite >= CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(CASE
                                                                                          WHEN upper(rtrim(right(t1.primarysite, 3))) = '***' THEN '000'
                                                                                          ELSE right(t1.primarysite, 3)
                                                                                      END) AS FLOAT64)
AND t3.beginrxtype <= 2
AND t3.endrxtype >= 2
AND t3.lookup_id = 4043
GROUP BY 2,
         3;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


UPDATE `hca-hin-dev-cur-ops`.edwcr.cr_patient_tumor AS tu
SET primary_site_surgery_summary_id = vt_lkp16.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_tumor_stg AS stg
CROSS JOIN vt_lkp16
CROSS JOIN vt_psss
WHERE tu.tumor_id = stg.tumorid
  AND upper(rtrim(stg.mstdefsurgprimsumm)) = upper(rtrim(vt_lkp16.lookup_code))
  AND vt_psss.groupid = CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(vt_lkp16.lookup_sub_code) AS FLOAT64)
  AND vt_psss.tumorid = stg.tumorid;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


UPDATE `hca-hin-dev-cur-ops`.edwcr.cr_patient_tumor AS tu
SET primary_site_surgery_summary_id =
  (SELECT vt_lkp16.master_lookup_sid
   FROM vt_lkp16
   WHERE upper(rtrim(vt_lkp16.lookup_code)) = '-99' )
WHERE tu.primary_site_surgery_summary_id IS NULL;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_psss;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_lkp16;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vt_pssh CLUSTER BY groupid,
                                   tumorid AS
SELECT max(t3.group_id) AS groupid,
       t3.lookup_id,
       t1.tumorid
FROM `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_tumor_stg AS t1
INNER JOIN `hca-hin-dev-cur-ops`.edwcr_staging.ref_lookup_groups_stg AS t3 ON t3.beginhisto <= CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(left(t1.histology, 4)) AS FLOAT64)
AND t3.endhisto >= CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(left(t1.histology, 4)) AS FLOAT64)
AND t3.beginprimarysite <= CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(CASE
                                                                                            WHEN upper(rtrim(right(t1.primarysite, 3))) = '***' THEN '000'
                                                                                            ELSE right(t1.primarysite, 3)
                                                                                        END) AS FLOAT64)
AND t3.endprimarysite >= CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(CASE
                                                                                          WHEN upper(rtrim(right(t1.primarysite, 3))) = '***' THEN '000'
                                                                                          ELSE right(t1.primarysite, 3)
                                                                                      END) AS FLOAT64)
AND t3.beginrxtype <= 2
AND t3.endrxtype >= 2
AND t3.lookup_id = 4043
GROUP BY 2,
         3;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


UPDATE `hca-hin-dev-cur-ops`.edwcr.cr_patient_tumor AS tu
SET primary_site_surgery_hospital_id = vt_lkp22.master_lookup_sid
FROM `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_tumor_stg AS stg
CROSS JOIN vt_lkp22
CROSS JOIN vt_pssh
WHERE tu.tumor_id = stg.tumorid
  AND upper(rtrim(stg.mstdefsurgprimhosp)) = upper(rtrim(vt_lkp22.lookup_code))
  AND vt_pssh.groupid = CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(vt_lkp22.lookup_sub_code) AS FLOAT64)
  AND vt_pssh.tumorid = stg.tumorid;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


UPDATE `hca-hin-dev-cur-ops`.edwcr.cr_patient_tumor AS tu
SET primary_site_surgery_hospital_id =
  (SELECT vt_lkp22.master_lookup_sid
   FROM vt_lkp22
   WHERE upper(rtrim(vt_lkp22.lookup_code)) = '-99' )
WHERE tu.primary_site_surgery_hospital_id IS NULL;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_pssh;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE vt_lkp22;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CR_Patient_Tumor');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF