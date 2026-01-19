CREATE OR REPLACE VIEW edwpsc_views.`pv_factbillvendorclaimstatushistory`
AS SELECT
  `pv_factbillvendorclaimstatushistory`.billvendorclaimstatushistorykey,
  `pv_factbillvendorclaimstatushistory`.sourceprimarykeyvalue,
  `pv_factbillvendorclaimstatushistory`.claimnumber,
  `pv_factbillvendorclaimstatushistory`.claimkey,
  `pv_factbillvendorclaimstatushistory`.tracenumber,
  `pv_factbillvendorclaimstatushistory`.patienticn,
  `pv_factbillvendorclaimstatushistory`.claimtotal,
  `pv_factbillvendorclaimstatushistory`.category277,
  `pv_factbillvendorclaimstatushistory`.status277,
  `pv_factbillvendorclaimstatushistory`.statdate,
  `pv_factbillvendorclaimstatushistory`.reporttype,
  `pv_factbillvendorclaimstatushistory`.processdate,
  `pv_factbillvendorclaimstatushistory`.payid,
  `pv_factbillvendorclaimstatushistory`.paysubid,
  `pv_factbillvendorclaimstatushistory`.claimrank,
  `pv_factbillvendorclaimstatushistory`.dwlastupdatedatetime,
  `pv_factbillvendorclaimstatushistory`.sourcesystemcode,
  `pv_factbillvendorclaimstatushistory`.insertedby,
  `pv_factbillvendorclaimstatushistory`.inserteddtm,
  `pv_factbillvendorclaimstatushistory`.modifiedby,
  `pv_factbillvendorclaimstatushistory`.modifieddtm,
  `pv_factbillvendorclaimstatushistory`.trackstat,
  `pv_factbillvendorclaimstatushistory`.addstatus,
  `pv_factbillvendorclaimstatushistory`.delchrgloop,
  `pv_factbillvendorclaimstatushistory`.stc12addstat,
  `pv_factbillvendorclaimstatushistory`.coid
  FROM
    edwpsc_base_views.`pv_factbillvendorclaimstatushistory`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pv_factbillvendorclaimstatushistory`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;