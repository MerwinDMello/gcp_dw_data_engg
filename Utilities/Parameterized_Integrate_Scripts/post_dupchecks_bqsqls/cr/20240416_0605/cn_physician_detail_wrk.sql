DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_physician_detail_wrk.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #####################################################################################
-- #  TARGET TABLE		: EDWCR_STAGING.CN_Physician_Detail_STG_WRK	       #
-- #  TARGET  DATABASE	   	: EDWCR	 						#
-- #  SOURCE		   	: EDWCR_STAGING.CN_Physician_Detail_STG			#
-- #	                                                                        	#
-- #  INITIAL RELEASE	   	: 								#
-- #  PROJECT             	: 	 		    					#
-- #  ------------------------------------------------------------------------		#
-- #                                                                              	#
-- #####################################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CN_PHYSICIAN_DETAIL;;
 --' FOR SESSION;;
 /* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','CN_Physician_Detail_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Truncate WRK Table */ BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr_staging.cn_physician_detail_stg_wrk;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Populate the WRK table with redcords for which Physician_ID is NOT NULL (This loads the data from DimMedicalSpecialist Table) */ BEGIN
SET _ERROR_CODE = 0;


INSERT INTO `hca-hin-dev-cur-ops`.edwcr_staging.cn_physician_detail_stg_wrk (physician_id, physician_name, physician_phone_num, dw_last_update_date_time)
SELECT cn_physician_detail_stg.physician_id,
       cn_physician_detail_stg.physician_name,
       cn_physician_detail_stg.physician_phone_num,
       datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_physician_detail_stg
WHERE cn_physician_detail_stg.physician_id IS NOT NULL;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Populate the WRK table with unique redcords for which Physician_ID is NULL */ /* This will insert the unique records. eg. If there are two records with phone Number NULL then it will insert only one record. */ BEGIN
SET _ERROR_CODE = 0;


INSERT INTO `hca-hin-dev-cur-ops`.edwcr_staging.cn_physician_detail_stg_wrk (physician_id, physician_name, physician_phone_num, dw_last_update_date_time)
SELECT cn_physician_detail_stg.physician_id,
       cn_physician_detail_stg.physician_name,
       cn_physician_detail_stg.physician_phone_num,
       datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_physician_detail_stg
WHERE cn_physician_detail_stg.physician_id IS NULL
  AND (cn_physician_detail_stg.physician_name IS NOT NULL
       OR cn_physician_detail_stg.physician_phone_num IS NOT NULL) QUALIFY row_number() OVER (PARTITION BY upper(trim(cn_physician_detail_stg.physician_name)),
                                                                                                           upper(trim(cn_physician_detail_stg.physician_phone_num))
                                                                                              ORDER BY upper(cn_physician_detail_stg.physician_phone_num) DESC) = 1;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/*
INSERT INTO  edwcr_staging.CN_Physician_Detail_STG_WRK
(
Physician_Id,
Physician_Name,
Physician_Phone_Num,
DW_Last_Update_Date_Time
)
SELECT
NULL as Physician_ID,
Gynecologist,
GynecologistPhone,
Current_Timestamp(0) as DW_Last_Update_Date_Time
from edwcr_staging.CN_Patient_STG
where Gynecologist is not null
qualify row_number() over(partition by TRIM(Gynecologist),TRIM(GynecologistPhone) order by GynecologistPhone desc)=1
UNION ALL
SELECT
NULL as Physician_ID,
PrimaryCarePhysician,
PCPPhone,
Current_Timestamp(0) as DW_Last_Update_Date_Time
from edwcr_staging.CN_Patient_STG
where PrimaryCarePhysician is not null
qualify row_number() over(partition by TRIM(PrimaryCarePhysician),TRIM(PCPPhone) order by PCPPhone desc)=1
UNION ALL
SELECT
NULL as Physician_ID,
EndTreatmentPhysician,
CAST(NULL as varchar(12)) AS Physician_Phone_Num,
Current_Timestamp(0) as DW_Last_Update_Date_Time
from edwcr_staging.CN_Patient_Tumor_STG
where EndTreatmentPhysician is not null
UNION ALL
SELECT
NULL as Physician_ID,
Hematologist,
CAST(NULL as varchar(12)) AS Physician_Phone_Num,
Current_Timestamp(0) as DW_Last_Update_Date_Time
from edwcr_staging.CN_Patient_Heme_STG
where Hematologist is not null;


.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;


*/ -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','CN_Physician_Detail_STG_WRK');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF;;