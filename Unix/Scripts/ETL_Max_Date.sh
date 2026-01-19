if [ -f ${IDFDIR}/${Job_Name}.date ];
then 
rm -f ${IDFDIR}/${Job_Name}.date
fi

bteq << EOF >> /dev/null

.SET ERROROUT STDOUT
.RUN FILE ${LOGONDIR}/HDW_AC
.SET TITLEDASHES OFF
.EXPORT REPORT FILE=${IDFDIR}/${Job_Name}.date
SELECT Extract_Date_Time (TITLE'') FROM ${NCR_STG_SCHEMA}.Ref_Increment_Date WHERE Job_Name='${Job_Name}';
.LOGOFF
.QUIT

EOF

chmod 777 ${IDFDIR}/${Job_Name}.date


null_check=`cat ${IDFDIR}/${Job_Name}.date | sed 's/ //g'`
if [ "$null_check" = "" ]
then
	export Last_Update='1900-01-01'
else
	export Last_Update=`cat ${IDFDIR}/${Job_Name}.date`
fi

echo "Source Data Extraction Date - $Last_Update"