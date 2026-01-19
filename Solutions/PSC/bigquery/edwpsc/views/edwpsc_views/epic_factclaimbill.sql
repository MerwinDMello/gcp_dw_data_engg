CREATE OR REPLACE VIEW edwpsc_views.`epic_factclaimbill`
AS SELECT
  `epic_factclaimbill`.claimbillkey,
  `epic_factclaimbill`.claimkey,
  `epic_factclaimbill`.claimnumber,
  `epic_factclaimbill`.visitnumber,
  `epic_factclaimbill`.regionkey,
  `epic_factclaimbill`.coid,
  `epic_factclaimbill`.coidconfigurationkey,
  `epic_factclaimbill`.servicingproviderkey,
  `epic_factclaimbill`.claimpayer1iplankey,
  `epic_factclaimbill`.facilitykey,
  `epic_factclaimbill`.invoicenumber,
  `epic_factclaimbill`.invoicestatus,
  `epic_factclaimbill`.billtypekey,
  `epic_factclaimbill`.iplankey,
  `epic_factclaimbill`.billmessage,
  `epic_factclaimbill`.batchnumber,
  `epic_factclaimbill`.billdatekey,
  `epic_factclaimbill`.billtime,
  `epic_factclaimbill`.sourceaprimarykeyvalue,
  `epic_factclaimbill`.sourcearecordlastupdated,
  `epic_factclaimbill`.sourcebprimarykeyvalue,
  `epic_factclaimbill`.sourcebrecordlastupdated,
  `epic_factclaimbill`.dwlastupdatedatetime,
  `epic_factclaimbill`.sourcesystemcode,
  `epic_factclaimbill`.insertedby,
  `epic_factclaimbill`.inserteddtm,
  `epic_factclaimbill`.modifiedby,
  `epic_factclaimbill`.modifieddtm
  FROM
    edwpsc_base_views.`epic_factclaimbill`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`epic_factclaimbill`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;