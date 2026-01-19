##########################
## Variable Declaration ##
##########################

log_month=`TZ=CST+1440 date +%Y%m`

###########################
### Archive Current Log ###
###########################

echo "Archiving the Current Log"
cp ${LOGDIR}/${Job_Name}.log ${LOGDIR}/Archive/${Job_Name}_${logdate}.log
chmod 755 ${LOGDIR}/Archive/${Job_Name}_${logdate}.log

####################
### Zip Old Logs ###
####################

if [ -f ${LOGDIR}/*.log_${log_month}* ];
then
echo "Compressing Log Files for `TZ=CST+1440 date +%b' '%Y`"
tar -cvf ${ARCHIVE_DIR}/Logs/Logs_${log_month}.tar ${LOGDIR}/*.log_${log_month}*
gzip ${ARCHIVE_DIR}/Logs/Logs_${log_month}.tar 
rm -f ${LOGDIR}/*.log_${log_month}*
fi