__author__ = 'G'


import sys
import urllib
import pandas as pd
import argparse
import json
import datetime


# url = "https://www.nomisweb.co.uk/api/v01/dataset/NM_18_1.data.csv?geography=1946157199...1946157245&date=latest&age=MAKE|Aged%2016-24|1;2&duration=MAKE|Up%20to%206%20months|1...7,MAKE|Over%206%20months%20and%20up%20to%20a%20year|8;9,MAKE|Over%201%20year|10...16&sex=5,6&measures=20100,20206&select=geography_code,geography_name,sex_name,age_name,duration_name,measures_name,obs_value,date"
# output_path = "tempYouthUnemployment.csv"


def download(url, outPath):
    dName = outPath

    try:
        socket = urllib.request.urlopen(url)
    except urllib.error.HTTPError as e:
        errfile.write(str(now()) + ' csv download HTTPError is ' + str(e.code) + ' . End progress\n')
        logfile.write(str(now()) + ' error and end progress\n')
        sys.exit('csv download HTTPError = ' + str(e.code))
    except urllib.error.URLError as e:
        errfile.write(str(now()) + ' csv download URLError is ' + str(e.args) + ' . End progress\n')
        logfile.write(str(now()) + ' error and end progress\n')
        sys.exit('csv download URLError = ' + str(e.args))
    except Exception:
        print('csv file download error')
        import traceback
        errfile.write(str(now()) + ' generic exception: ' + str(traceback.format_exc()) + ' . End progress\n')
        logfile.write(str(now()) + ' error and end progress\n')
        sys.exit('generic exception: ' + traceback.format_exc())

    # load this csv file
    logfile.write(str(now()) + ' csv file loading\n')
    print('csv file loading------')
    df = pd.read_csv(socket, dtype='unicode')

    # save csv file
    logfile.write(str(now()) + ' writing to file\n')
    print('writing to file ' + dName)
    df.to_csv(dName, index=False)
    logfile.write(str(now()) + ' has been extracted and saved as ' + str(dName) + '\n')
    print('Requested data has been extracted and saved as ' + dName)
    logfile.write(str(now()) + ' finished\n')
    print("finished")

def now():
    return datetime.datetime.now()


parser = argparse.ArgumentParser(
    description='Extract online NOMIS Data, specially for "Youth Unemployment Data" to .csv file.')
parser.add_argument("--generateConfig", "-g", help="generate a config file called config_tempYouthUnemployment.json",
                    action="store_true")
parser.add_argument("--configFile", "-c", help="path for config file")
args = parser.parse_args()

if args.generateConfig:
    obj = {
        "url": "https://www.nomisweb.co.uk/api/v01/dataset/NM_18_1.data.csv?geography=1946157199...1946157245&date=latest&age=MAKE|Aged%2016-24|1;2&duration=MAKE|Up%20to%206%20months|1...7,MAKE|Over%206%20months%20and%20up%20to%20a%20year|8;9,MAKE|Over%201%20year|10...16&sex=5,6&measures=20100,20206&select=geography_code,geography_name,sex_name,age_name,duration_name,measures_name,obs_value,date",
        "outPath": "tempYouthUnemployment.csv"
    }

    logfile = open("log_tempYouthUnemployment.log", "w")
    logfile.write(str(now()) + ' start\n')

    errfile = open("err_tempYouthUnemployment.err", "w")

    with open("config_tempYouthUnemployment.json", "w") as outfile:
        json.dump(obj, outfile, indent=4)
        logfile.write(str(now()) + ' config file generated and end\n')
        sys.exit("config file generated")

if args.configFile == None:
    args.configFile = "config_tempYouthUnemployment.json"

with open(args.configFile) as json_file:
    oConfig = json.load(json_file)

    logfile = open('log_' + oConfig["outPath"].split('.')[0] + '.log', "w")
    logfile.write(str(now()) + ' start\n')

    errfile = open('err_' + oConfig["outPath"].split('.')[0] + '.err', "w")

    logfile.write(str(now()) + ' read config file\n')
    print("read config file")

download(oConfig["url"], oConfig["outPath"])
