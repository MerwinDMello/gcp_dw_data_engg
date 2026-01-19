Select 'PBMCR350-20' ||','||cast(zeroifnull(Count(1)) as varchar(20)) || ',' AS Source_String from
edwpbs.Fact_RCOM_PARS_Credit_Refund
where Date_Sid = cast((add_months(current_date, -1) (format 'yyyymm')) as char(6))