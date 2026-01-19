CREATE OR REPLACE VIEW edwpsc_views.`onbase_factrefund`
AS SELECT
  `onbase_factrefund`.documenthandle,
  `onbase_factrefund`.totalrefundamount,
  `onbase_factrefund`.coid,
  `onbase_factrefund`.claimnumber,
  `onbase_factrefund`.refundamount,
  `onbase_factrefund`.refundtype,
  `onbase_factrefund`.refundsource,
  `onbase_factrefund`.reasonforrefund,
  `onbase_factrefund`.payableto,
  `onbase_factrefund`.teamleadapproval,
  `onbase_factrefund`.mgrapprovalusername,
  `onbase_factrefund`.directorapprovalusername,
  `onbase_factrefund`.requestedbydate,
  `onbase_factrefund`.adjustmentcode,
  `onbase_factrefund`.otherapprovalusername,
  `onbase_factrefund`.requestedby,
  `onbase_factrefund`.dateoffinalapproval,
  `onbase_factrefund`.numberofclaims,
  `onbase_factrefund`.status,
  `onbase_factrefund`.insertedby,
  `onbase_factrefund`.inserteddtm,
  `onbase_factrefund`.modifiedby,
  `onbase_factrefund`.modifieddtm,
  `onbase_factrefund`.requestedbythreefour,
  `onbase_factrefund`.dwlastupdatedatetime,
  `onbase_factrefund`.region,
  `onbase_factrefund`.onbaserefunddatakey
  FROM
    edwpsc_base_views.`onbase_factrefund`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`onbase_factrefund`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;