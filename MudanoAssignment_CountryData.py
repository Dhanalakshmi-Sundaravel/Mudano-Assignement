from collections import OrderedDict

import urllib3
import json

import csv
import pandas as pd
from sqlalchemy import create_engine

#Function to extract data from API as JSON data
def getjson(url):
    http = urllib3.PoolManager()
    response = http.request('GET', url)
    return response.data

#loop through all 6 pages and get whole dataframe
for i in range(1,7):
    data = getjson("http://api.worldbank.org/v2/country/?format=json&page="+str(i)+"")
    dataJson = json.loads(data)
    dataNormalize = pd.json_normalize(dataJson[1])
    if i == 1:
        whole_countryData = dataNormalize
    else:
        whole_countryData = whole_countryData.append(dataNormalize)
    #print(dataNormalize.info())

# to understand the data
print(whole_countryData.info())
print(whole_countryData.describe())
whole_countryData.to_excel("data_normalize.xlsx")

Country_data = whole_countryData[['id','iso2Code','name','capitalCity','longitude','latitude','region.id','adminregion.id','incomeLevel.id','lendingType.id']]
Region_data = whole_countryData[['region.id','region.iso2code','region.value']]
AdminRegion_data = whole_countryData[['adminregion.id','adminregion.iso2code','adminregion.value']]
IncomeLevel_data = whole_countryData[['incomeLevel.id','incomeLevel.iso2code','incomeLevel.value']]

IncomeLevel_data.loc[IncomeLevel_data['incomeLevel.value'] == 'High income', ['incomeLevel.valueCode']] = 5
IncomeLevel_data.loc[IncomeLevel_data['incomeLevel.value'] == 'Upper middle income', ['incomeLevel.valueCode']] = 4
IncomeLevel_data.loc[IncomeLevel_data['incomeLevel.value'] == 'Lower middle income', ['incomeLevel.valueCode']] = 3
IncomeLevel_data.loc[IncomeLevel_data['incomeLevel.value'] == 'Low income', ['incomeLevel.valueCode']] = 2
IncomeLevel_data.loc[IncomeLevel_data['incomeLevel.value'] == 'Aggregates', ['incomeLevel.valueCode']] = 0
IncomeLevel_data.loc[IncomeLevel_data['incomeLevel.value'] == 'Not classified', ['incomeLevel.valueCode']] = 0

IncomeLevel_data.head(10)

LendingType_data = whole_countryData[['lendingType.id','lendingType.iso2code','lendingType.value']]

#Finding unique rows
Region_data = Region_data.drop_duplicates()
AdminRegion_data = AdminRegion_data.drop_duplicates()
IncomeLevel_data = IncomeLevel_data.drop_duplicates()
LendingType_data = LendingType_data.drop_duplicates()

creds = {'usr': 'root',
             'pwd': 'ethili$04',
             'hst': '127.0.0.1',
             'prt': 3306,
             'dbn': 'dhan'}
# MySQL conection string.
connstr = 'mysql+mysqlconnector://{usr}:{pwd}@{hst}:{prt}/{dbn}'
# Create sqlalchemy engine for MySQL connection.
engine = create_engine(connstr.format(**creds))


Country_data.to_sql(name='COUNTRIES', con=engine, if_exists='append', index=False)
Region_data.to_sql(name='REGION', con=engine, if_exists='append', index=False)
AdminRegion_data.to_sql(name='ADMINREGION', con=engine, if_exists='append', index=False)
IncomeLevel_data.to_sql(name='INCOMELEVEL', con=engine, if_exists='append', index=False)
LendingType_data.to_sql(name='LENDINGTYPE', con=engine, if_exists='append', index=False)




# Read addresses from mCSV file.
text = list(csv.reader(open('GEPData.csv'), skipinitialspace=True))

# Replace all commas which are not used as field separators.
# Remove additional whitespace.
for idx, row in enumerate(text):
    text[idx] = [i.strip().replace(',', '') for i in row]

# Store data into a DataFrame.
df = pd.DataFrame(data=text, columns=['Country Name', 'Country Code','Indicator Name','Indicator Code','1999','2000','2001','2002','2003','2004','2005','2006','2007','2008','2009','2010','2011','2012','2013','2014','2015','2016','2017','2018','2019','2020','2021','2022'])
df = df[['Country Name','Country Code','2018','2019','2020','2021','2022']]

print(df)

print(df.info())

print(df.describe())
# Write DataFrame to MySQL using the engine (connection) created.
df.to_sql(name='ged', con=engine, if_exists='append', index=False)

print("done")
