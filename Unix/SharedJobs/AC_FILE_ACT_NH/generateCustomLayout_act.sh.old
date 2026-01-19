#!/bin/bash
#
# This script will generate a DMExpress layout dynamically based on the values
# of the following environment variables:
#    JOBNAME
#    P_Number_of_Fields
#    P_Control_Total_Field
#    P_Field_End_Positions
#
################################################################################
LAYOUT_FILENAME="${IDF_DIR}/AggregationLayout_act_$JOBNAME.txt"

echo "========================================================================="
echo "Running with the following parameters:"
echo "JOBNAME = ${JOBNAME}"
echo "P_ACT_Control_Total_Field = ${P_ACT_Control_Total_Field}"
echo "P_ACT_Number_of_Fields = ${P_ACT_Number_of_Fields}"
echo "P_ACT_Control_Total_type = ${P_ACT_Control_Total_type}"
echo "P_ACT_Delimiter = ${P_ACT_Delimiter}"
echo "P_ACT_Field_End_Positions = ${P_ACT_Field_End_Positions}"
echo "========================================================================="
echo 

# Check if we are working with delimted fields or fixed.
if [ "${P_ACT_Delimiter}" = "" ]
then

   RECORD_LAYOUT="/RECORDLAYOUT AggregationInputLayout {"
    
    # Generate a record layout with fixed postion fields
    END_POSITIONS=(`echo ${P_ACT_Field_End_Positions} | sed -e 's|,| |g' `)
    DUMMY_CNT=${#END_POSITIONS[@]}
    DUMMY_CNT=$((DUMMY_CNT-1))
    DUMMY_NUM=1
    
    RECORD_LAYOUT="${RECORD_LAYOUT} dummy_1 ${END_POSITIONS[0]}"
    
    while [ ${DUMMY_NUM} -le ${DUMMY_CNT} ]
    do 
       PREV_FIELD_NUM=$((DUMMY_NUM-1))
       START=${END_POSITIONS[$PREV_FIELD_NUM]}
       END=${END_POSITIONS[$DUMMY_NUM]}
       
       LEN=$((END-START))
       DUMMY_NUM=$((DUMMY_NUM +1))
	   RECORD_LAYOUT="${RECORD_LAYOUT}, dummy_${DUMMY_NUM} ${LEN}"
    done
    
	# Based on the Control fields, replace the dummy fields with real fields. 
    FIELD_NUM=1
    FIELD_POSITIONS=`echo ${P_ACT_Control_Total_Field} | sed -e 's|,| |g'`
    for FIELD in ${FIELD_POSITIONS}
    do 
		
       RECORD_LAYOUT=`echo ${RECORD_LAYOUT} | sed -e "s|dummy_${FIELD}|field_${FIELD_NUM}|"`
       FIELD_NUM=$((FIELD_NUM +1))
    done
	
#	FIELD_NUM=$((FIELD_NUM +1))
    while [ ${FIELD_NUM} -le 20 ]
    do
       RECORD_LAYOUT="${RECORD_LAYOUT}, field_${FIELD_NUM} char 1"
       FIELD_NUM=$((FIELD_NUM +1))
    done

    RECORD_LAYOUT="${RECORD_LAYOUT} }"
    
else

    # Generate a record layout with delimited fields..
    RECORD_LAYOUT="/DELIMITEDRECORDLAYOUT AggregationInputLayout {"
    
    # Build a record layout with the correct number of dummy fields
    DUMMY_COUNT=1
    while [ ${DUMMY_COUNT} -le ${P_ACT_Number_of_Fields} ]
    do 
       if [ $DUMMY_COUNT -gt 1 ]
       then
           RECORD_LAYOUT="${RECORD_LAYOUT},"
       fi
       
       RECORD_LAYOUT="${RECORD_LAYOUT} dummy_${DUMMY_COUNT}"
       DUMMY_COUNT=$((DUMMY_COUNT +1))
    done
    # Based on the Control fields, replace the dummy fields with real fields. 
    FIELD_NUM=1
    FIELD_POSITIONS=`echo ${P_ACT_Control_Total_Field} | sed -e 's|,| |g'`
    for FIELD in ${FIELD_POSITIONS}
    do 
       RECORD_LAYOUT=`echo ${RECORD_LAYOUT} | sed -e "s|dummy_${FIELD}|field_${FIELD_NUM}|"`
       FIELD_NUM=$((FIELD_NUM +1))
    done
    
    # Add any missing metric fields
    while [ ${FIELD_NUM} -le 20 ]
    do 
       RECORD_LAYOUT="${RECORD_LAYOUT}, field_${FIELD_NUM}"
       FIELD_NUM=$((FIELD_NUM +1))
    done
         
    RECORD_LAYOUT="${RECORD_LAYOUT} }"
fi

echo  "${RECORD_LAYOUT}" > ${LAYOUT_FILENAME}
