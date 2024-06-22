import psycopg2
import pandas as pd
import time

def create_database():
    '''This function when called creates a database connection to database accounts and
    returns cconnection as conn and cursor as cur
    Calling: conn,cur=create_database()
    '''

    conn=psycopg2.connect('host=127.0.0.1 dbname=postgres user=postgres password=root')
    cur=conn.cursor()
    conn.set_session(autocommit=True)

    #create database 
    cur.execute('Drop database if exists accounts')
    cur.execute('Create database  accounts')

    conn.close()

    #create connection to accounts database
    conn=psycopg2.connect('host=127.0.0.1 dbname=accounts user=postgres password=root')
    cur=conn.cursor()
    conn.set_session(autocommit=True)

    return conn, cur

#Load datasets

Wealth_AccountData= pd.read_csv(r"E:\mydata\New folder (2)\Postgress\Wealth-AccountData.csv")


Wealth_AccountCountry= pd.read_csv(r"E:\mydata\New folder (2)\Postgress\Wealth-AccountsCountry.csv")

Wealth_AccountSeries=pd.read_csv(r"E:\mydata\New folder (2)\Postgress\Wealth-AccountSeries.csv")

Wealth_AccountSeries.head(5)

#Filter needed columns
AccountData = Wealth_AccountData[['Country Name','Country Code','Series Name', 'Series Code', '1995 [YR1995]', '2000 [YR2000]', '2005 [YR2005]', '2010 [YR2010]', '2015 [YR2015]', '2018 [YR2018]']]

AccountCountry = Wealth_AccountCountry[['Code', 'Short Name', 'Table Name', 'Long Name', 'Currency Unit']]

AccountSeries = Wealth_AccountSeries[['Code','Topic', 'Indicator Name', 'Long definition']]


#Create dataabse
conn,cur = create_database()

#Create tables
AccountData_table_create=('Create table if not exists AccountData ("Country Name" varchar , "Country Code" varchar, "Series Name" varchar, "Series Code" varchar, "1995 [YR1995]" numeric, "2000 [YR2000]" numeric, "2005 [YR2005]" numeric, "2010 [YR2010]" numeric, "2015 [YR2015]" numeric, "2018 [YR2018]" numeric ) ')
cur.execute(AccountData_table_create)

AccountCountry_table_create = ('Create table if not exists AccountsCountry (Code varchar PRIMARY KEY, "Short Name" varchar, "Table Name" varchar, "Long Name" varchar, "Currency Unit" varchar)')
cur.execute(AccountCountry_table_create)

AccountSeries_table_create = ('Create table if not exists AccountsSeries (Code varchar , Topic varchar, "Indicator Name" varchar, "Long definition" varchar)')
cur.execute(AccountSeries_table_create)


#Insert data
AccountData_table_insert=('Insert into AccountData("Country Name", "Country Code", "Series Name", "Series Code", "1995 [YR1995]", "2000 [YR2000]" , "2005 [YR2005]", "2010 [YR2010]", "2015 [YR2015]", "2018 [YR2018]") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)')

for i, row in AccountData.iterrows():
    cur.execute(AccountData_table_insert, list(row))



AccountsCountry_table_insert=('Insert into AccountsCountry(Code, "Short Name", "Table Name", "Long Name", "Currency Unit") VALUES (%s, %s, %s, %s, %s)')

for i, row in AccountCountry.iterrows():
    cur.execute(AccountsCountry_table_insert, list(row))

AccountSeries_table_insert=('Insert into AccountsSeries(Code, Topic, "Indicator Name", "Long definition") VALUES (%s, %s, %s, %s)')

for i, row in AccountSeries.iterrows():
    cur.execute(AccountSeries_table_insert, list(row))


#Retrieve data using joins

querry=('''select ad."Country Name", ad."Country Code", ac."Long Name",ac."Currency Unit",ad."Series Name", 
 ad."Series Code",asr."Indicator Name",ad."2000 [YR2000]", ad."2010 [YR2010]", ad."2018 [YR2018]"  from AccountData ad
JOIN AccountsCountry ac ON ad."Country Code"=ac.Code
JOIN AccountsSeries asr ON ad."Series Code"=asr.Code''')

start_time=time.time()
cur.execute(querry)

end_time=time.time()

row=cur.fetchone()
while row is not None:
    print(row)
    row=cur.fetchone()

time_taken=end_time -start_time
print("Time taken to execute querry: {}".format(time_taken))



#Creating dimension tables

Create_dimamount_table=('Create table if not exists dimamount("Country Code" varchar, "2000 [YR2000]" numeric, "2010 [YR2010]" numeric, "2018 [YR2018]" numeric)')
cur.execute(Create_dimamount_table)


Create_dimCountry_table=('Create table if not exists dimcountry("Country Code" varchar, "Country Name" varchar, "Long Name" varchar)')
cur.execute(Create_dimCountry_table)

Create_dimseries_table=('Create table if not exists dimseries("Series Name" varchar, "Series Code" varchar, "Indicator Name" varchar)')
cur.execute(Create_dimseries_table)

Create_dimcurrency_table=('Create table if not exists dimcurrency("Country Code" varchar, "Currency Unit" varchar,  "Country Name" varchar)')
cur.execute(Create_dimcurrency_table)

#Creating FactTable

Create_factCurrency_table=('Create table if not exists factcurrency("Country Code" varchar, "Series code" varchar)')
cur.execute(Create_factCurrency_table)



#Inserting values
Insert_dimAmount_table=('''Insert into dimamount("Country Code","2000 [YR2000]", "2010 [YR2010]", "2018 [YR2018]")
select "Country Code", "2000 [YR2000]", "2010 [YR2010]", "2018 [YR2018]" from AccountData''')


cur.execute(Insert_dimAmount_table)


Insert_dimCountry_table=('''Insert into dimcountry("Country Code", "Country Name", "Long Name") 
select ad."Country Code", ad."Country Name", ac."Long Name" from AccountData ad
JOIN AccountsCountry ac ON ad."Country Code"=ac.Code ''')

cur.execute(Insert_dimCountry_table)

Insert_dimSeries_table=('''Insert into dimseries("Series Name", "Series Code", "Indicator Name")
select ad."Series Name", ad."Series Code", asr."Indicator Name" from AccountData ad
JOIN AccountsSeries asr ON ad."Series Code"=asr.Code''')

cur.execute(Insert_dimSeries_table)

Insert_dimCurrency_table=('''Insert into dimcurrency("Currency Unit", "Country Name", "Country Code")
select  ac."Currency Unit",  ad."Country Name", ad."Country Code" from AccountData ad
JOIN AccountsCountry ac ON ad."Country Code"=ac.Code''')

cur.execute(Insert_dimCurrency_table)

Insert_factCurrency_table=('''Insert into factcurrency("Country Code", "Series code")
select ad."Country Code",asr.Code from AccountData ad
JOIN AccountsSeries asr ON ad."Series Code"=asr.Code''')

cur.execute(Insert_factCurrency_table)


#Retrieve using star schema
starquerry=('''select dimcountry."Country Name", factcurrency."Country Code", dimcountry."Long Name",dimcurrency."Currency Unit", dimseries."Indicator Name",dimseries."Series Name", 
factcurrency."Series code",dimamount."2000 [YR2000]", dimamount."2010 [YR2010]", dimamount."2018 [YR2018]"  from factcurrency
JOIN dimcurrency ON dimcurrency."Country Code"=factcurrency."Country Code"
JOIN dimcountry ON dimcountry."Country Code"=factcurrency."Country Code"
JOIN dimseries ON dimseries."Series Code"=factcurrency."Series code"
JOIN dimamount ON dimamount."Country Code"=factcurrency."Country Code" ''')

start_time1=time.time()

cur.execute(starquerry)

end_time1=time.time()

row=cur.fetchone()

while row is not None:
    print(row)
    row=cur.fetchone()


time_taken=end_time -start_time
print("Time taken to execute querry: {}".format(time_taken))


