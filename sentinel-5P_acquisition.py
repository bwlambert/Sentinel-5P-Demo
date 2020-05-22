import os
import json
import csv
 
polygon = "POLYGON((113.70 30.11,114.90 30.11,114.90 30.97,113.70 30.97,113.70 30.11))"

datespan = "2019-02-02T00:00:00.000Z TO 2019-02-05T00:00:00.000Z"

product_type = "NO2___"

# Available Product Types:
# Product               Data file descriptor	
# UV Aerosol Index	AER_AI	
# Aerosol Layer Height	AER_LH
# Carbon monoxide (CO)	CO____	
# Cloud	                CLOUD_	
# Formaldehyde (HCHO)	HCHO__	
# Methane (CH4)column	CH4___
# Nitrogen oxide (NO2)  NO2___	
# Sulphur dioxide (SO2)	SO2___	
# Ozone (O3)            O3____
# Tropospheric Ozone 	O3_TCL
# Additional information here:
# http://www.tropomi.eu/data-products/level-2-products


reqnum = 0
startdate = datespan.split(' ')[0][0:10] 
stopdate = datespan.split(' ')[2][0:10]
datespanl = f"{startdate}_{stopdate}"
query_results_file = f"query_results_{product_type}_{datespanl}_{reqnum}.json" 
processed = 0

# First Search:
ec = "\\"
qs = f"wget --no-check-certificate --user=s5pguest --password=s5pguest --output-document={query_results_file} \"https://s5phub.copernicus.eu/dhus/search?q=beginposition:[{datespan}] AND (footprint:{ec}\"Intersects({polygon}){ec}\") AND producttype:L2__{product_type}&rows=100&start=0&format=json\""

# Call wget
os.system(qs)

uuid_links = []

with open(query_results_file) as f:
    data = json.load(f)

total = int(data['feed']['opensearch:totalResults'])
print(total)
while (processed < total):
    c = len( data['feed']['entry'])
    for i in range(0,c):
        title = data['feed']['entry'][i]['title']
        if(("NO2___" in title)):
            print(title)
            link = data['feed']['entry'][i]['link'][0]
            uuid = link['href']
            print(uuid)
            t = uuid.split("\'")
            uuid = t[1] 
            print(uuid)
            uuid_links.append([link,uuid])
            processed += 1

    if( processed < total): 
        reqnum += 1
        query_results_file = f"query_results_{product_type}_{datespanl}_{reqnum}.json" 
    
        #Next Search:
        qs = f"wget --no-check-certificate --user=s5pguest --password=s5pguest --output-document={query_results_file} \"https://s5phub.copernicus.eu/dhus/search?q=beginposition:[{datespan}] AND (footprint:{ec}\"Intersects({polygon}){ec}\") AND producttype:L2__{product_type}&rows=100&start=0&format=json\""
        
        os.system(qs)
        
        with open(query_results_file) as f:
            data = json.load(f)
    


with open(f"query_links_{product_type}_{datespanl}_{reqnum}.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerows(uuid_links)

for i in uuid_links:
        uuid = i[1] 
        print("Requesting file",uuid)
        ds = f"wget --content-disposition --continue --user=s5pguest --password=s5pguest \"https://s5phub.copernicus.eu/dhus/odata/v1/Products(\'{uuid}\')/\$value\""
        os.system(ds)

