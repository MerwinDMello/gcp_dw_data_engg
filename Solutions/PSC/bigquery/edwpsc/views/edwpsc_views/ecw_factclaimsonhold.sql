CREATE OR REPLACE VIEW edwpsc_views.`ecw_factclaimsonhold`
AS SELECT
  `ecw_factclaimsonhold`.claimsonholdkey,
  `ecw_factclaimsonhold`.loaddatekey,
  `ecw_factclaimsonhold`.claimkey,
  `ecw_factclaimsonhold`.claimnumber,
  `ecw_factclaimsonhold`.claimbalance,
  `ecw_factclaimsonhold`.regionkey,
  `ecw_factclaimsonhold`.artivaruleid,
  `ecw_factclaimsonhold`.artivaruledesc,
  `ecw_factclaimsonhold`.artivaruleowner,
  `ecw_factclaimsonhold`.artivarulecategoryid,
  `ecw_factclaimsonhold`.artivaholdcodeid,
  `ecw_factclaimsonhold`.artivaholdcodedescription,
  `ecw_factclaimsonhold`.artivaholdcodetype,
  `ecw_factclaimsonhold`.artivappiidpe,
  `ecw_factclaimsonhold`.artivappitypepe,
  `ecw_factclaimsonhold`.artivappiidedi,
  `ecw_factclaimsonhold`.artivappitypeedi,
  `ecw_factclaimsonhold`.artivappiphase,
  `ecw_factclaimsonhold`.artivarulecreatedate,
  `ecw_factclaimsonhold`.artivaruleactiveflag,
  `ecw_factclaimsonhold`.artivaruleglobalflag,
  `ecw_factclaimsonhold`.artivarulesource,
  `ecw_factclaimsonhold`.artivarulelastupdatedate,
  `ecw_factclaimsonhold`.artivarulelastupdatenote,
  `ecw_factclaimsonhold`.artivarulelastupdateby,
  `ecw_factclaimsonhold`.artivaholdcoderiskreportflag,
  `ecw_factclaimsonhold`.dwlastupdatedatetime,
  `ecw_factclaimsonhold`.sourcesystemcode,
  `ecw_factclaimsonhold`.insertedby,
  `ecw_factclaimsonhold`.inserteddtm,
  `ecw_factclaimsonhold`.modifiedby,
  `ecw_factclaimsonhold`.modifieddtm,
  `ecw_factclaimsonhold`.coid
  FROM
    edwpsc_base_views.`ecw_factclaimsonhold`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_factclaimsonhold`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;