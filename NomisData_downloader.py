__author__ = 'G'


import sys
sys.path.append('../harvesterlib')

import argparse
import json

import now
import getapi
import downloadapi


# url = "https://www.nomisweb.co.uk/api/v01/dataset/NM_18_1.data.csv?geography=1946157199...1946157245&date=latest&age=MAKE|Aged%2016-24|1;2&duration=MAKE|Up%20to%206%20months|1...7,MAKE|Over%206%20months%20and%20up%20to%20a%20year|8;9,MAKE|Over%201%20year|10...16&sex=5,6&measures=20100,20206&select=geography_code,geography_name,sex_name,age_name,duration_name,measures_name,obs_value,date"
# output_path = "tempYouthUnemployment.csv"


parser = argparse.ArgumentParser(
    description='Extract online NOMIS Data, specially for "Youth Unemployment Data" to .csv file.')
parser.add_argument("--generateConfig", "-g", help="generate a config file called config_tempYouthUnemployment.json",
                    action="store_true")
parser.add_argument("--configFile", "-c", help="path for config file")
args = parser.parse_args()

if args.generateConfig:
    obj = {
        #"url": "https://www.nomisweb.co.uk/api/v01/dataset/NM_18_1.data.csv?geography=1946157199...1946157245&date=latest&age=MAKE|Aged%2016-24|1;2&duration=MAKE|Up%20to%206%20months|1...7,MAKE|Over%206%20months%20and%20up%20to%20a%20year|8;9,MAKE|Over%201%20year|10...16&sex=5,6&measures=20100,20206&select=geography_code,geography_name,sex_name,age_name,duration_name,measures_name,obs_value,date",
        "outPath": "tempYouthUnemployment.csv",
        "date": ["Latest", "2015-07", "2015-04", "2015-09", "2014-03", "2012-05"],
        "colFields": ["Geography_code", "geography_name", "sex_name", "Age_name", "duration_name", "Measures_name", "Obs_value", "Date"],
        "primaryKeyCol": ["Geography_code", "sex_name", "Age_name", "duration_name", "Measures_name", "Date"],#[0, 2, 3, 4, 5, 7],
        "digitCheckCol": ["Obs_value"],#[6],
        "noDigitRemoveFields": ["Obs_value"],#[6]
    }

    logfile = open("log_tempYouthUnemployment.log", "w")
    logfile.write(str(now.now()) + ' start\n')

    errfile = open("err_tempYouthUnemployment.err", "w")

    with open("config_tempYouthUnemployment.json", "w") as outfile:
        json.dump(obj, outfile, indent=4)
        logfile.write(str(now.now()) + ' config file generated and end\n')
        sys.exit("config file generated")

if args.configFile == None:
    args.configFile = "config_tempYouthUnemployment.json"

with open(args.configFile) as json_file:
    oConfig = json.load(json_file)

    logfile = open('log_' + oConfig["outPath"].split('.')[0] + '.log', "w")
    logfile.write(str(now.now()) + ' start\n')

    errfile = open('err_' + oConfig["outPath"].split('.')[0] + '.err', "w")

    logfile.write(str(now.now()) + ' read config file\n')
    print("read config file")

url = getapi.getapi(oConfig["date"], oConfig["colFields"], logfile, errfile)

downloadapi.download(url, oConfig["outPath"], oConfig["primaryKeyCol"], oConfig["digitCheckCol"], oConfig["noDigitRemoveFields"], logfile, errfile)
