CREATE OR REPLACE VIEW edwpsc_views.`artiva_factaffiliations`
AS SELECT
  `artiva_factaffiliations`.artivaaffiliationskey,
  `artiva_factaffiliations`.artivaproviderkey,
  `artiva_factaffiliations`.artivagroupkey,
  `artiva_factaffiliations`.artivainstitutionkey,
  `artiva_factaffiliations`.provideraffiliationlevel,
  `artiva_factaffiliations`.hospitalaffiliation,
  `artiva_factaffiliations`.affiliationstartdate,
  `artiva_factaffiliations`.affiliationtermdate,
  `artiva_factaffiliations`.affiliationtype,
  `artiva_factaffiliations`.affiliationstate,
  `artiva_factaffiliations`.hospitalaffiliationcategory,
  `artiva_factaffiliations`.affiliationspecialty,
  `artiva_factaffiliations`.affiliationactiveflag,
  `artiva_factaffiliations`.dwlastupdatedatetime,
  `artiva_factaffiliations`.sourcesystemcode,
  `artiva_factaffiliations`.insertedby,
  `artiva_factaffiliations`.inserteddtm,
  `artiva_factaffiliations`.modifiedby,
  `artiva_factaffiliations`.modifieddtm
  FROM
    edwpsc_base_views.`artiva_factaffiliations`
;