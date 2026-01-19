#Script Name	: TMS_Temp.sh
#Description	: This Script creates Temp directories )
#Created By 	: Syntel
#Created On	: 02.10.2020
#Syntax	        : TMS_Temp.sh {file frequency mode. EX: TMS_Temp.sh DAILY}



#Change History:


#Version No.		Modified By		Modified Date   Comments
############		############		##############	 #########

#1.0			Syntel		        02.10.2020       Created

############################################################################################


 if [ -d TMS_${1}_Temp ]; 
then
  ##rmdir TMS_Temp
	rm -rf TMS_${1}_Temp
fi

mkdir TMS_${1}_Temp
chmod 777 TMS_${1}_Temp

if [ -d TMS_${1}_Temp_Zip ];
then  
    rm -rf TMS_${1}_Temp_Zip
fi

mkdir TMS_${1}_Temp_Zip
chmod 777 TMS_${1}_Temp_Zip
