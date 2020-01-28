import json
from cassandra.cluster import Cluster

class Id:
    def __init__(self, oid):
        self.oid = oid

class Description:
    def __init__(self, country, sector, industry):
        self.country = country
        self.sector = sector
        self.industry = industry

class Ratio:
    def __init__(self, quick, current):
        self.quick = quick
        self.current = current

class Performance:
    def __init__(self, year, half_year, month, week):
        self.year = year
        self.half_year = half_year
        self.month = month
        self.week = week

Company = []
Oid = []
Price = []
Earnings_Date = []
Descriptions = []
Twenty_Day = []
TwoH_Day = []
Fifty_Day = []
Fifty_Two_Week = []
Analyst_Recom = []
Average_True_Range = []
Average_Volume = []
Beta = []
Change = []
EPS_ttm = []
ROI = []
Ratios = []
Performances = []

def Insert_Company(comp_val):
    return "Insert into Company (comp) values (" + comp_val + ")"

def Insert_Price(price_val):
    return "Insert into Price (p) values (" + price_val + ")"

def Insert_Earnings(earnings_val):
    return "Insert into Earnings Date (ed) values (" + earnings_val + ")"

def Insert_Twenty(twenty_val):
    return "Insert into Company (comp) values (" + twenty_val + ")"

def Insert_TwoH(twoh_val):
    return "Insert into Company (comp) values (" + twoh_val + ")"

def Insert_Fifty(fifty_val):
    return "Insert into Company (comp) values (" + fifty_val + ")"

def Create_Database(name):
    return "CREATE KEYSPACE IF NOT EXISTS " + name \
           + "WITH REPLICATION = {'class: SimpleStrategy', 'replication_factor': 3};" \
             " USE "+name+ ";"





with open('C:\\Users\\Ambre\\Desktop\\ESILV\\A4\\NoSQL\\stocks.json', 'r') as json_file:
    for line in json_file:
        json_string = json.loads(line)
        #print(json_string)
        #print("Company: " + str(json_string["Company"]))
        Oid.append(Id(str(json_string["_id"]["$oid"])))
        Company.append(str(json_string["Company"]))
        Price.append(str(json_string["Price"]))
        Earnings_Date.append(str(json_string["Earnings Date"]))
        Descriptions.append(Description(str(json_string["description"]["Country"]),
                           str(json_string["description"]["Sector"]),
                           str(json_string["description"]["Industry"])))
        Twenty_Day.append(str(json_string["20-Day Simple Moving Average"]))
        TwoH_Day.append(str(json_string["200-Day Simple Moving Average"]))
        Fifty_Day.append(str(json_string["50-Day"]))
        Fifty_Two_Week.append(str(json_string["52-Week"]))
        Analyst_Recom.append(str(json_string["Analyst Recom"]))
        Average_True_Range.append(str(json_string["Average True Range"]))
        Average_Volume.append(str(json_string["Average Volume"]))
        Beta.append(str(json_string["Beta"]))
        Change.append(str(json_string["Change"]))
        EPS_ttm.append(str(json_string["EPS ttm"]))
        ROI.append(str(json_string["ROI"]))

        Ratios.append(Ratio(str(json_string["ratio"]["quick"]),
                            str(json_string["ratio"]["current"])))

        Performances.append(Performance(str(json_string["performance"]["Year"]),
                                        str(json_string["performance"]["Half Year"]),
                                        str(json_string["performance"]["Month"]),
                                        str(json_string["performance"]["Week"])))

for i in Oid:
    print(i.oid)


