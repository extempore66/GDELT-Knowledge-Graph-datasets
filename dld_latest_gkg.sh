#!/bin/bash

#exp_url men_url
#exp_file_name men_file_name

##Dates
## Local
#date +%Y%m%d%H%M00
## UTC
#date -u  +%Y%m%d%H%M00

read   gkg_url gkg_file_name <<< $(

	curl http://data.gdeltproject.org/gdeltv2/lastupdate.txt | awk ' { 
		line = $0;
		pos = index(line, "http");
		##print  "line is: " line;
		if ($0  ~ /gkg/  )  { 
			url_gkg = substr(line, pos);
			# this matches the last occurence of / (forward slash) then returns RSTART as the index of that occurence
			match(url_gkg, /\/[^\/]*$/); 
			file_name_gkg = substr(url_gkg, RSTART + 1);
			url_list["gkg"] = 3;
			file_name["gkg"] = 3;
		}
	
		if ( length(url_list) == 1 ){
			#print  url_exp; print url_men; 
			print url_gkg;
			#print file_name_exp;print file_name_men;
			print file_name_gkg;	
		}

	}'
)

echo   ' / gkg_url: ' $gkg_url ' / gkg_file_name: ' $gkg_file_name

cd ../data
## curl replaces the file by default bu twe wanna avoid writing too many times
if [[ -f "$gkg_file_name" ]]; then
    echo $gkg_file_name ' already exists'
else
	curl $gkg_url --output $gkg_file_name
	wait
	echo 'done waiting for curling gkg file'
	tar -xvf $gkg_file_name
	wait 
	echo 'done waiting for unzipping gkg file'
	rm $gkg_file_name
fi

cd ../utils
