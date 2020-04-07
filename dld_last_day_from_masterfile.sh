#!/bin/bash

## Usage ./dld_last_day_from_masterfile.sh

#exp_url men_url
#exp_file_name men_file_name

###Dates
### Local
##date +%Y%m%d%H%M00
### UTC
##date -u  +%Y%m%d%H%M00


curl http://data.gdeltproject.org/gdeltv2/masterfilelist.txt | awk ' { 
		line = $0;
		pos = index(line, "http");
		##print  "line is: " line;
		
		if ($0  ~ /gkg/  )  { 
			url_gkg = substr(line, pos);
			url_list[counter] = url_gkg;
			counter++;
		}
}
END { 
		print url_list[(counter - 96)] > "gdelt_last_96_list.txt"
		for ( i = counter - 95; i < counter; i++ ){
			print url_list[i] >> "gdelt_last_96_list.txt"
		}
}
'
wait
echo "the master file has been copied locally with the last 24 hours files"


input="${PWD}/gdelt_last_96_list.txt"
echo ${input}
cd ../data

while IFS= read -r line  # IFS prevents trimming of the whitespaces before and after the filename (or something like that)
do	  
	#echo $line
	zipFile=${line#http://*/*/}
	#echo $zipFile
	if [[ -f "$zipFile" ]]; then
    	echo $zipFile ' already exists'
    else
    	echo $zipFile " don't exist"
    	curl $line --output $zipFile
		wait
		echo 'done waiting for curling gkg file'
		tar -xvf $zipFile
		wait 
		echo 'done waiting for unzipping gkg file'
		rm $zipFile
    fi
done < "$input"

cd ../utils

