DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/mt_ref_lookup_code.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #####################################################################################
-- #  TARGET TABLE		: EDWCR.REF_LOOKUP_CODE		              	#
-- #  TARGET  DATABASE	   	: EDWCR	 						#
-- #  SOURCE		   	: EDWCR_STAGING.REF_LOOKUP_CODE_STG			#
-- #	                                                                        	#
-- #  INITIAL RELEASE	   	: 								#
-- #  PROJECT             	: 	 		    					#
-- #  ------------------------------------------------------------------------		#
-- #    VERSION:1 Changes related to CES-14669(03/06/2020)                                            					#
-- #####################################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_REF_LOOKUP_CODE;;
 --' FOR SESSION;;
 /* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','REF_LOOKUP_CODE_Stg');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Delete the records from Core table which don't exist in the Staging table */ BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Insert the new records into the Core Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY p.lookup_id) AS master_lookup_sid,
                                     p.lookup_id AS lookup_id,
                                     substr(p.lookup_code, 1, 100) AS lookup_code,
                                     p.lookup_sub_code AS lookup_sub_code,
                                     substr(p.lookup_desc, 1, 500) AS lookup_desc,
                                     p.source_system_code AS source_system_code,
                                     p.dw_last_update_date_time
   FROM
     (SELECT CASE
                 WHEN upper(ref_lookup_code_stg.lookup_name) LIKE '%VITALSTAT%' THEN 1
                 WHEN upper(ref_lookup_code_stg.lookup_name) LIKE 'RACE%' THEN 2
                 WHEN ref_lookup_code_stg.lookup_id = 307 THEN 3
                 WHEN ref_lookup_code_stg.lookup_id = 263 THEN 4
                 WHEN ref_lookup_code_stg.lookup_id = 438 THEN 5
                 WHEN ref_lookup_code_stg.lookup_id = 8004 THEN 6
                 WHEN ref_lookup_code_stg.lookup_id = 473 THEN 7
                 WHEN ref_lookup_code_stg.lookup_id = 158 THEN 8
                 WHEN ref_lookup_code_stg.lookup_id = 160 THEN 9
                 WHEN ref_lookup_code_stg.lookup_id = 388 THEN 10
                 WHEN ref_lookup_code_stg.lookup_id = 152 THEN 11
                 WHEN ref_lookup_code_stg.lookup_id = 125 THEN 12
                 WHEN upper(ref_lookup_code_stg.lookup_name) LIKE '%PRIMPAYER' THEN 13
                 WHEN ref_lookup_code_stg.lookup_id = 999914 THEN 14
                 WHEN upper(ref_lookup_code_stg.lookup_name) LIKE 'RT_MODALITY%' THEN 15
                 WHEN upper(ref_lookup_code_stg.lookup_name) LIKE 'GRADE' THEN 16
                 WHEN ref_lookup_code_stg.lookup_id = 526 THEN 17
                 WHEN ref_lookup_code_stg.lookup_id = 206 THEN 18
                 WHEN ref_lookup_code_stg.lookup_id = 242 THEN 19
                 WHEN ref_lookup_code_stg.lookup_id = 243 THEN 20
                 WHEN ref_lookup_code_stg.lookup_id = 567 THEN 21
                 WHEN ref_lookup_code_stg.lookup_id = 244 THEN 22
                 WHEN ref_lookup_code_stg.lookup_id = 999923 THEN 23
                 WHEN ref_lookup_code_stg.lookup_id = 481 THEN 24
                 WHEN ref_lookup_code_stg.lookup_id = 483 THEN 25
                 WHEN ref_lookup_code_stg.lookup_id = 334 THEN 26
                 WHEN ref_lookup_code_stg.lookup_id = 589 THEN 27
                 WHEN ref_lookup_code_stg.lookup_id = 482 THEN 28
                 WHEN ref_lookup_code_stg.lookup_id = 289 THEN 29
                 WHEN ref_lookup_code_stg.lookup_id = 283 THEN 31
                 WHEN ref_lookup_code_stg.lookup_id = 486 THEN 32
                 WHEN ref_lookup_code_stg.lookup_id = 46 THEN 39
                 WHEN ref_lookup_code_stg.lookup_id = 8261 THEN 40
                 WHEN ref_lookup_code_stg.lookup_id = 8001 THEN 42
                 WHEN ref_lookup_code_stg.lookup_id = 4118 THEN 43
                 WHEN ref_lookup_code_stg.lookup_id = 49 THEN 44
                 WHEN ref_lookup_code_stg.lookup_id = 306 THEN 45
                 WHEN ref_lookup_code_stg.lookup_id = 999946 THEN 46
                 WHEN upper(ref_lookup_code_stg.lookup_name) LIKE '%ROUTE' THEN 47
                 WHEN upper(ref_lookup_code_stg.lookup_name) LIKE '%DOSEUNITS' THEN 48
                 WHEN upper(ref_lookup_code_stg.lookup_name) LIKE '%PRESCA_CONF' THEN 52
             END AS lookup_id,
             max(ref_lookup_code_stg.lookup_code) AS lookup_code,
             max(ref_lookup_code_stg.lookup_sub_code) AS lookup_sub_code,
             max(ref_lookup_code_stg.lookup_desc) AS lookup_desc,
             max(ref_lookup_code_stg.source_system_code) AS source_system_code,
             datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.ref_lookup_code_stg
      GROUP BY 1,
               upper(ref_lookup_code_stg.lookup_code),
               upper(ref_lookup_code_stg.lookup_sub_code),
               upper(ref_lookup_code_stg.lookup_desc),
               upper(ref_lookup_code_stg.source_system_code),
               6) AS p
   WHERE p.lookup_id IS NOT NULL ) AS ms ON mt.master_lookup_sid = ms.master_lookup_sid
AND mt.lookup_id = ms.lookup_id
AND mt.lookup_code = ms.lookup_code
AND (upper(coalesce(mt.lookup_sub_code, '0')) = upper(coalesce(ms.lookup_sub_code, '0'))
     AND upper(coalesce(mt.lookup_sub_code, '1')) = upper(coalesce(ms.lookup_sub_code, '1')))
AND (upper(coalesce(mt.lookup_desc, '0')) = upper(coalesce(ms.lookup_desc, '0'))
     AND upper(coalesce(mt.lookup_desc, '1')) = upper(coalesce(ms.lookup_desc, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (master_lookup_sid,
        lookup_id,
        lookup_code,
        lookup_sub_code,
        lookup_desc,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.master_lookup_sid, ms.lookup_id, ms.lookup_code, ms.lookup_sub_code, ms.lookup_desc, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT master_lookup_sid
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code
      GROUP BY master_lookup_sid
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code');

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

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY upper(max(ref_lookup_code_stg.lookup_code))) +
     (SELECT coalesce(max(ref_lookup_code.master_lookup_sid), 0)
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code) AS master_lookup_sid,
                                     41 AS lookup_id,
                                     substr(max(ref_lookup_code_stg.lookup_code), 1, 100) AS lookup_code,
                                     max(ref_lookup_code_stg.lookup_sub_code) AS lookup_sub_code,
                                     substr(max(ref_lookup_code_stg.lookup_desc), 1, 500) AS lookup_desc,
                                     max(ref_lookup_code_stg.source_system_code) AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.ref_lookup_code_stg
   WHERE ref_lookup_code_stg.lookup_id = 8261
   GROUP BY 2,
            upper(substr(ref_lookup_code_stg.lookup_code, 1, 100)),
            upper(ref_lookup_code_stg.lookup_sub_code),
            upper(substr(ref_lookup_code_stg.lookup_desc, 1, 500)),
            upper(ref_lookup_code_stg.source_system_code),
            7) AS ms ON mt.master_lookup_sid = ms.master_lookup_sid
AND mt.lookup_id = ms.lookup_id
AND mt.lookup_code = ms.lookup_code
AND (upper(coalesce(mt.lookup_sub_code, '0')) = upper(coalesce(ms.lookup_sub_code, '0'))
     AND upper(coalesce(mt.lookup_sub_code, '1')) = upper(coalesce(ms.lookup_sub_code, '1')))
AND (upper(coalesce(mt.lookup_desc, '0')) = upper(coalesce(ms.lookup_desc, '0'))
     AND upper(coalesce(mt.lookup_desc, '1')) = upper(coalesce(ms.lookup_desc, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (master_lookup_sid,
        lookup_id,
        lookup_code,
        lookup_sub_code,
        lookup_desc,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.master_lookup_sid, ms.lookup_id, ms.lookup_code, ms.lookup_sub_code, ms.lookup_desc, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT master_lookup_sid
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code
      GROUP BY master_lookup_sid
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code');

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

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY upper(o.lookup_code)) +
     (SELECT coalesce(max(ref_lookup_code.master_lookup_sid), 0)
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code) AS master_lookup_sid,
                                     o.lookup_id AS lookup_id,
                                     substr(o.lookup_code, 1, 100) AS lookup_code,
                                     o.lookup_sub_code AS lookup_sub_code,
                                     substr(o.lookup_desc, 1, 500) AS lookup_desc,
                                     'M' AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT 34 AS lookup_id,
             ref_lookup_code_stg.lookup_code,
             ref_lookup_code_stg.lookup_sub_code,
             ref_lookup_code_stg.lookup_desc,
             ref_lookup_code_stg.source_system_code,
             datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.ref_lookup_code_stg
      WHERE ref_lookup_code_stg.lookup_id = 481
      UNION ALL SELECT 30 AS lookup_id,
                       max(ref_lookup_code_stg.lookup_code) AS lookup_code,
                       max(ref_lookup_code_stg.lookup_sub_code) AS lookup_sub_code,
                       max(ref_lookup_code_stg.lookup_desc) AS lookup_desc,
                       max(ref_lookup_code_stg.source_system_code) AS source_system_code,
                       datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.ref_lookup_code_stg
      WHERE ref_lookup_code_stg.lookup_id = 284
      GROUP BY 1,
               upper(ref_lookup_code_stg.lookup_code),
               upper(ref_lookup_code_stg.lookup_sub_code),
               upper(ref_lookup_code_stg.lookup_desc),
               upper(ref_lookup_code_stg.source_system_code),
               6) AS o) AS ms ON mt.master_lookup_sid = ms.master_lookup_sid
AND mt.lookup_id = ms.lookup_id
AND mt.lookup_code = ms.lookup_code
AND (upper(coalesce(mt.lookup_sub_code, '0')) = upper(coalesce(ms.lookup_sub_code, '0'))
     AND upper(coalesce(mt.lookup_sub_code, '1')) = upper(coalesce(ms.lookup_sub_code, '1')))
AND (upper(coalesce(mt.lookup_desc, '0')) = upper(coalesce(ms.lookup_desc, '0'))
     AND upper(coalesce(mt.lookup_desc, '1')) = upper(coalesce(ms.lookup_desc, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (master_lookup_sid,
        lookup_id,
        lookup_code,
        lookup_sub_code,
        lookup_desc,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.master_lookup_sid, ms.lookup_id, ms.lookup_code, ms.lookup_sub_code, ms.lookup_desc, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT master_lookup_sid
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code
      GROUP BY master_lookup_sid
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code');

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

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY upper(o.lookup_code)) +
     (SELECT coalesce(max(ref_lookup_code.master_lookup_sid), 0)
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code) AS master_lookup_sid,
                                     o.lookup_id AS lookup_id,
                                     substr(o.lookup_code, 1, 100) AS lookup_code,
                                     o.lookup_sub_code AS lookup_sub_code,
                                     substr(o.lookup_desc, 1, 500) AS lookup_desc,
                                     'M' AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT 33 AS lookup_id,
             max(lc.lookup_code) AS lookup_code,
             CAST(max(lg.group_id) AS STRING) AS lookup_sub_code,
             max(lc.lookup_desc) AS lookup_desc
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.ref_lookup_groups_stg AS lg
      INNER JOIN `hca-hin-dev-cur-ops`.edwcr_staging.ref_lookup_code_stg AS lc ON lg.lookup_id = lc.lookup_id
      AND lg.group_id = lc.group_id
      WHERE lg.beginrxtype <= 1.0
        AND lg.endrxtype >= 1.0
        AND lg.lookup_id = 4043.0
      GROUP BY 1,
               upper(lc.lookup_code),
               upper(CAST(lg.group_id AS STRING)),
               upper(lc.lookup_desc)
      UNION ALL SELECT 35 AS lookup_id,
                       max(lc.lookup_code) AS lookup_code,
                       CAST(max(lg.group_id) AS STRING) AS lookup_sub_code,
                       max(lc.lookup_desc) AS lookup_desc
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.ref_lookup_groups_stg AS lg
      INNER JOIN `hca-hin-dev-cur-ops`.edwcr_staging.ref_lookup_code_stg AS lc ON lg.lookup_id = lc.lookup_id
      AND lg.group_id = lc.group_id
      WHERE lg.beginrxtype <= 6.0
        AND lg.endrxtype >= 6.0
        AND lg.lookup_id = 4043.0
      GROUP BY 1,
               upper(lc.lookup_code),
               upper(CAST(lg.group_id AS STRING)),
               upper(lc.lookup_desc)
      UNION ALL SELECT 36 AS lookup_id,
                       max(lc.lookup_code) AS lookup_code,
                       CAST(max(lg.group_id) AS STRING) AS lookup_sub_code,
                       max(lc.lookup_desc) AS lookup_desc
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.ref_lookup_groups_stg AS lg
      INNER JOIN `hca-hin-dev-cur-ops`.edwcr_staging.ref_lookup_code_stg AS lc ON lg.lookup_id = lc.lookup_id
      AND lg.group_id = lc.group_id
      WHERE lg.beginrxtype <= 5.0
        AND lg.endrxtype >= 5.0
        AND lg.lookup_id = 4043.0
      GROUP BY 1,
               upper(lc.lookup_code),
               upper(CAST(lg.group_id AS STRING)),
               upper(lc.lookup_desc)
      UNION ALL SELECT 37 AS lookup_id,
                       max(lc.lookup_code) AS lookup_code,
                       CAST(max(lg.group_id) AS STRING) AS lookup_sub_code,
                       max(lc.lookup_desc) AS lookup_desc
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.ref_lookup_groups_stg AS lg
      INNER JOIN `hca-hin-dev-cur-ops`.edwcr_staging.ref_lookup_code_stg AS lc ON lg.lookup_id = lc.lookup_id
      AND lg.group_id = lc.group_id
      WHERE lg.beginrxtype <= 0.0
        AND lg.endrxtype >= 0.0
        AND lg.lookup_id = 4043.0
      GROUP BY 1,
               upper(lc.lookup_code),
               upper(CAST(lg.group_id AS STRING)),
               upper(lc.lookup_desc)
      UNION ALL SELECT 38 AS lookup_id,
                       max(lc.lookup_code) AS lookup_code,
                       CAST(max(lg.group_id) AS STRING) AS lookup_sub_code,
                       max(lc.lookup_desc) AS lookup_desc
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.ref_lookup_groups_stg AS lg
      INNER JOIN `hca-hin-dev-cur-ops`.edwcr_staging.ref_lookup_code_stg AS lc ON lg.lookup_id = lc.lookup_id
      AND lg.group_id = lc.group_id
      WHERE lg.beginrxtype <= 4.0
        AND lg.endrxtype >= 4.0
        AND lg.lookup_id = 4043.0
      GROUP BY 1,
               upper(lc.lookup_code),
               upper(CAST(lg.group_id AS STRING)),
               upper(lc.lookup_desc)
      UNION ALL SELECT 49 AS lookup_id,
                       max(lc.lookup_code) AS lookup_code,
                       CAST(max(lg.group_id) AS STRING) AS lookup_sub_code,
                       max(lc.lookup_desc) AS lookup_desc
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.ref_lookup_groups_stg AS lg
      INNER JOIN `hca-hin-dev-cur-ops`.edwcr_staging.ref_lookup_code_stg AS lc ON lg.lookup_id = lc.lookup_id
      AND lg.group_id = lc.group_id
      WHERE lg.beginrxtype <= 2.0
        AND lg.endrxtype >= 2.0
        AND lg.lookup_id = 4043.0
      GROUP BY 1,
               upper(lc.lookup_code),
               upper(CAST(lg.group_id AS STRING)),
               upper(lc.lookup_desc)
      UNION ALL SELECT 50 AS lookup_id,
                       max(lc.lookup_code) AS lookup_code,
                       CAST(max(lg.group_id) AS STRING) AS lookup_sub_code,
                       max(lc.lookup_desc) AS lookup_desc
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.ref_lookup_groups_stg AS lg
      INNER JOIN `hca-hin-dev-cur-ops`.edwcr_staging.ref_lookup_code_stg AS lc ON lg.lookup_id = lc.lookup_id
      AND lg.group_id = lc.group_id
      WHERE lg.beginrxtype <= 2.0
        AND lg.endrxtype >= 2.0
        AND lg.lookup_id = 4043.0
      GROUP BY 1,
               upper(lc.lookup_code),
               upper(CAST(lg.group_id AS STRING)),
               upper(lc.lookup_desc)) AS o) AS ms ON mt.master_lookup_sid = ms.master_lookup_sid
AND mt.lookup_id = ms.lookup_id
AND mt.lookup_code = ms.lookup_code
AND (upper(coalesce(mt.lookup_sub_code, '0')) = upper(coalesce(ms.lookup_sub_code, '0'))
     AND upper(coalesce(mt.lookup_sub_code, '1')) = upper(coalesce(ms.lookup_sub_code, '1')))
AND (upper(coalesce(mt.lookup_desc, '0')) = upper(coalesce(ms.lookup_desc, '0'))
     AND upper(coalesce(mt.lookup_desc, '1')) = upper(coalesce(ms.lookup_desc, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (master_lookup_sid,
        lookup_id,
        lookup_code,
        lookup_sub_code,
        lookup_desc,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.master_lookup_sid, ms.lookup_id, ms.lookup_code, ms.lookup_sub_code, ms.lookup_desc, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT master_lookup_sid
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code
      GROUP BY master_lookup_sid
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code');

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

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY stg.lookup_sid) +
     (SELECT coalesce(max(ref_lookup_code.master_lookup_sid), 0)
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code) AS master_lookup_sid,
                                     stg.lookup_sid,
                                     substr(unk.lookup_code, 1, 100) AS lookup_code,
                                     unk.lookup_sub_code AS lookup_sub_code,
                                     substr(unk.lookup_desc, 1, 500) AS lookup_desc,
                                     unk.source_system_code AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT DISTINCT ref_lookup_name.lookup_sid
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name) AS stg
   INNER JOIN
     (SELECT '-99' AS lookup_code,
             '-99' AS lookup_sub_code,
             'Unknown Description' AS lookup_desc,
             -99 AS group_id,
             'M' AS source_system_code) AS unk ON 1 = 1) AS ms ON mt.master_lookup_sid = ms.master_lookup_sid
AND mt.lookup_id = ms.lookup_sid
AND mt.lookup_code = ms.lookup_code
AND (upper(coalesce(mt.lookup_sub_code, '0')) = upper(coalesce(ms.lookup_sub_code, '0'))
     AND upper(coalesce(mt.lookup_sub_code, '1')) = upper(coalesce(ms.lookup_sub_code, '1')))
AND (upper(coalesce(mt.lookup_desc, '0')) = upper(coalesce(ms.lookup_desc, '0'))
     AND upper(coalesce(mt.lookup_desc, '1')) = upper(coalesce(ms.lookup_desc, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (master_lookup_sid,
        lookup_id,
        lookup_code,
        lookup_sub_code,
        lookup_desc,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.master_lookup_sid, ms.lookup_sid, ms.lookup_code, ms.lookup_sub_code, ms.lookup_desc, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT master_lookup_sid
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code
      GROUP BY master_lookup_sid
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code');

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

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY a.lookup_code) +
     (SELECT coalesce(max(ref_lookup_code.master_lookup_sid), 0)
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code) AS master_lookup_sid,
                                     a.lookup_id AS lookup_id,
                                     substr(a.lookup_code, 1, 100) AS lookup_code,
                                     CAST(NULL AS STRING) AS lookup_sub_code,
                                     substr(a.lookup_desc, 1, 500) AS lookup_desc,
                                     'M' AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT DISTINCT 51 AS lookup_id,
                      CASE
                          WHEN t1.lookup_desc IS NULL THEN format('%11d', -99)
                          ELSE t1.lookup_code
                      END AS lookup_code,
                      NULL AS lookup_sub_code,
                      CASE
                          WHEN t1.lookup_desc IS NULL THEN 'Unknown Description'
                          ELSE ltrim(rtrim(t1.lookup_desc))
                      END AS lookup_desc
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.ref_lookup_code_stg AS t1
      WHERE t1.lookup_id = 4005.0
        AND t1.group_id = 290
        AND t1.lookup_desc IS NOT NULL ) AS a) AS ms ON mt.master_lookup_sid = ms.master_lookup_sid
AND mt.lookup_id = ms.lookup_id
AND mt.lookup_code = ms.lookup_code
AND (upper(coalesce(mt.lookup_sub_code, '0')) = upper(coalesce(ms.lookup_sub_code, '0'))
     AND upper(coalesce(mt.lookup_sub_code, '1')) = upper(coalesce(ms.lookup_sub_code, '1')))
AND (upper(coalesce(mt.lookup_desc, '0')) = upper(coalesce(ms.lookup_desc, '0'))
     AND upper(coalesce(mt.lookup_desc, '1')) = upper(coalesce(ms.lookup_desc, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (master_lookup_sid,
        lookup_id,
        lookup_code,
        lookup_sub_code,
        lookup_desc,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.master_lookup_sid, ms.lookup_id, ms.lookup_code, ms.lookup_sub_code, ms.lookup_desc, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT master_lookup_sid
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code
      GROUP BY master_lookup_sid
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','REF_LOOKUP_CODE');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF;;