CREATE OR REPLACE VIEW edwpsc_views.`epic_factworkqueuechargereview`
AS SELECT
  `epic_factworkqueuechargereview`.regionkey,
  `epic_factworkqueuechargereview`.regionname,
  `epic_factworkqueuechargereview`.coid,
  `epic_factworkqueuechargereview`.facilitykey,
  `epic_factworkqueuechargereview`.facilityname,
  `epic_factworkqueuechargereview`.poscode,
  `epic_factworkqueuechargereview`.encounterid,
  `epic_factworkqueuechargereview`.encounterkey,
  `epic_factworkqueuechargereview`.claimkey,
  `epic_factworkqueuechargereview`.patientmrn,
  `epic_factworkqueuechargereview`.chargeslipnumber,
  `epic_factworkqueuechargereview`.servicedate,
  `epic_factworkqueuechargereview`.createddate,
  `epic_factworkqueuechargereview`.workqueuetype,
  `epic_factworkqueuechargereview`.workqueueid,
  `epic_factworkqueuechargereview`.workqueuename,
  `epic_factworkqueuechargereview`.enteredworkqueuedate,
  `epic_factworkqueuechargereview`.daysinworkqueue,
  `epic_factworkqueuechargereview`.totalbalanceamt,
  `epic_factworkqueuechargereview`.cptbalanceamt,
  `epic_factworkqueuechargereview`.cptcode,
  `epic_factworkqueuechargereview`.cptcodename,
  `epic_factworkqueuechargereview`.chargeline,
  `epic_factworkqueuechargereview`.reviewedbyuserkey,
  `epic_factworkqueuechargereview`.liabilityfinancialclass,
  `epic_factworkqueuechargereview`.liabilityfinancialclassname,
  `epic_factworkqueuechargereview`.liabilityiplankey,
  `epic_factworkqueuechargereview`.liabilityiplanname,
  `epic_factworkqueuechargereview`.epicfinancialclass,
  `epic_factworkqueuechargereview`.epicfinancialclassdesc,
  `epic_factworkqueuechargereview`.servicingproviderkey,
  `epic_factworkqueuechargereview`.servicingprovidername,
  `epic_factworkqueuechargereview`.activedeferredstatus,
  `epic_factworkqueuechargereview`.deferreddate,
  `epic_factworkqueuechargereview`.activedate,
  `epic_factworkqueuechargereview`.sourceaprimarykeyvalue,
  `epic_factworkqueuechargereview`.sourcebprimarykeyvalue,
  `epic_factworkqueuechargereview`.insertedby,
  `epic_factworkqueuechargereview`.inserteddtm,
  `epic_factworkqueuechargereview`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`epic_factworkqueuechargereview`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`epic_factworkqueuechargereview`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;