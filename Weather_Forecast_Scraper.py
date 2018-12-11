import requests as rq
from pprint import pprint
import json
from pandas.io.json import json_normalize
import pandas as pd
import datetime
import time
import civis
import os

#Zip code variable
zips = ["02118", 
        "02119", 
        "02120", 
        "02130", 
        "02134", 
        "02135", 
        "02445", 
        "02446",
        "02447",
        "02467",
        "02108",
        "02114",
        "02115",
        "02116",
        "02215",
        "02128",
        "02129",
        "02150",
        "02151",
        "02152",
        "02124",
        "02126",
        "02131",
        "02132",
        "02136",
        "02109",
        "02110",
        "02111",
        "02113",
        "02121",
        "02122",
        "02124",
        "02125",
        "02127",
        "02210"]     


#Load API key (will probs need to change this to an environmental variable once we get a location down)
akey = os.environ['API_KEY']

#Create empty data frame to append everything to
m_data = pd.DataFrame()

#get today's date for the scrape date variable
dt = datetime.datetime.now()

#For loop that iterates through a list of zips and makes api calls
for i in zips:
    #build the url using the predetermined end point as well as out iterated zip and the api key
    ur = "http://api.openweathermap.org/data/2.5/forecast?units=imperial&zip=" + i + ",us" + "&appid=" + akey
    #send the request for the api data
    resp = rq.get(ur)
    
    #parse the JSON data we just grabbed
    data = resp.json()    
    drill = data['list']
    #pprint(len(drill))
    for e in range(len(drill)):
    #for e in range(1):
        ds = drill[e]
        #Get the date and time of the forecast
        fore_tm = pd.DataFrame(data = [ds['dt_txt']], columns= ["fore_tm"])
        
        #Main weather info
        main_parse = json_normalize(ds['main'])
        main_nms = list(main_parse)
        for y in range(len(main_nms)):
            main_nms[y] = 'main_' + main_nms[y]
        main_parse.columns = main_nms
        
        #'Weather' weather info
        weath_parse = json_normalize(ds['weather'])
        weath_nms = list(weath_parse)
        for y in range(len(weath_nms)):
            weath_nms[y] = 'weath_' + weath_nms[y]
        weath_parse.columns = weath_nms
        
        #Cloud weather info
        cld_parse = json_normalize(ds['clouds'])
        cld_nms = list(cld_parse)
        for y in range(len(cld_nms)):
            cld_nms[y] = 'cld_' + cld_nms[y]
        cld_parse.columns = cld_nms
        
        
         #'wind' weather info
        wind_parse = json_normalize(ds['wind'])
        wind_nms = list(wind_parse)
        for y in range(len(wind_nms)):
            wind_nms[y] = 'wind_' + wind_nms[y]
        wind_parse.columns = wind_nms
        
        pr_chk = list(ds)
        if 'snow' in pr_chk:
            #If it's present, grab whatever it has
            sn_weath = json_normalize(ds['snow'])
            #Check to see if it includes both of the two possible variables
            #If not, it checks and returns the missing one as an NA
            if len(list(sn_weath)) != 2:
                if '1h' in list(sn_weath):
                    s_h1 = 'Y'
                else:
                    s_h1 = 'N'
                if '3h' in list(sn_weath):
                    s_h3 = 'Y'
                else:
                    s_h3 = 'N'
                if s_h1 == 'N':
                    sn_weath['1h'] = 'NA'
                if s_h3 =='N':
                    sn_weath['3h'] = 'NA'
            else:
                sn_weath = json_normalize(ds['snow'])
        
        else:
            #If it doesn't find either snow or rain info, it creates the df as a blank, NA filled one
            nosnow = {'1h':['NA'], '3h':['NA']}
            sn_weath = pd.DataFrame(nosnow)
        if 'rain' in pr_chk:
            #Same deal with rain information. Look for table, fill in possible blanks, or just pass a blank set of values
            rn_weath = json_normalize(ds['rain'])
            if len(list(rn_weath)) != 2:
                if '1h' in list(rn_weath):
                    r_h1 = 'Y'
                else:
                    r_h1 = 'N'
                if '3h' in list(rn_weath):
                    r_h3 = 'Y'
                else:
                    r_h3 = 'N'
                if r_h1 == 'N':
                    rn_weath['1h'] = 'NA'
                if r_h3 =='N':
                    rn_weath['3h'] = 'NA'
            else:
                rn_weath = json_normalize(ds['rain'])
        
        else:      
            norain = {'1h':['NA'], '3h':['NA']}
            rn_weath = pd.DataFrame(norain)       

        sn_nms = list(sn_weath)
        for a in range(len(sn_nms)):
            sn_nms[a] = 'snow_' + sn_nms[a]
        sn_weath.columns = sn_nms
    
        rn_nms = list(rn_weath)
        for j in range(len(rn_nms)):
            rn_nms[j] = 'rain_' + rn_nms[j]
        rn_weath.columns = rn_nms
    
        g_data = pd.concat([fore_tm, main_parse, weath_parse, cld_parse, wind_parse, sn_weath, rn_weath], axis = 1)
    
        #add the zip code
        g_data['zip'] = i
    
        #add the date scraped
        g_data['scrape_date'] = dt.strftime("%Y-%m-%d %H:%M")
    
        #append this to the original dummy data frame
        m_data = m_data.append(g_data)
        print("Finished with forecast " + str(e+1) + " of " + str(len(drill)) + " --- Zip " + str((zips.index(i)+1)) + " of " + str(len(zips)))
    
    print("Finished with zip " + i + " --- " + str((zips.index(i)+1)) + " of " + str(len(zips)))
    print("Sleeping for " + str(5) + " seconds...")
    time.sleep(1)
    print("Sleeping for " + str(4) + " seconds...")
    time.sleep(1)
    print("Sleeping for " + str(3) + " seconds...")
    time.sleep(1)
    print("Sleeping for " + str(2) + " seconds...")
    time.sleep(1)
    print("Sleeping for " + str(1) + " second...")
    time.sleep(1)
        

civis.io.dataframe_to_civis(m_data, database="City of Boston", table = "sandbox.openweathermap_forecast", existing_table_rows = "append")
