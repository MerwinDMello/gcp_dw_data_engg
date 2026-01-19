Dim StartTime, EndTime, strCommand, ad, rs 
StartTime = Now
Set ad = CreateObject("ADODB.Connection")
ad.ConnectionString = "DSN=EDW_PROD; User ID=KHU9683; Password=Mer82@dme; Persist Security Info=false; Session Mode=ANSI; "
ad.open
strCommand = "show table edwhr.candidate_onboarding_resource;"
Set rs = CreateObject("ADODB.Recordset")
Set rs = ad.Execute(strCommand)
EndTime = Now
if not (rs.BOF Or rs.EOF) then
    wscript.echo "Execution Time is : " & DateDiff("s",EndTime,StartTime) & " " & rs.Fields("Request Text").Value
end if
rs.Close
ad.Close