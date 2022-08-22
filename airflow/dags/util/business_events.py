from datetime import datetime, timedelta, date
from textwrap import dedent
from lib.mssql2017 import mssqlconfig
import pymssql
import random
from typing import List
# following two lines are commented out, if you want to debug in ipython, uncomment these two 
# lines to make library path visible to python kernel
# import sys
# sys.path.append("/opt/airflow/dags")


def createUsers() -> str:
    persons = []
    for a in range(random.randint(1, 10)):
        persons.append(Person.createPerson())
    sql = "\n".join([dedent(p.toinsertsql()) for p in persons])
    return sql


def createAddresses() -> str:
    addresses = []
    for a in range(random.randint(1, 10)):
        addresses.append(Address.createAddress())
    sql = "\n".join([dedent(a.toinsertsql()) for a in addresses])
    return sql


def createCustomers() -> str:
    customers = []
    for a in range(random.randint(1, 10)):
        customers.append(Customer.createCustomer())
        sql = "\n".join([dedent(c.toinsertsql()) for c in customers])
    return sql


def createOrders() -> str:
    orders = []
    for a in range(random.randint(1, 10)):
        orders.append(SalesOrderHeader.createSalesOrderHeader())
        sql = "begin transaction; \n declare @salesorderid as bigint;\n"+"\n".join([dedent(o.toinsertsql()) for o in orders]) +"\n;commit;"
    return sql


fnames = []
lnames = []
addresslines = []
cities = []
postalcodes = []
stateprovinceids = []
personids = []
addressids = []
customerids = []
specialofferproductids = []


def prepareRandomDataLists() -> None:
    """
    populate the lists with data ready to be chose
    """
    dbcon = pymssql.connect(
        server=mssqlconfig['servername'],
        user=mssqlconfig['username'],
        database='AdventureWorks2017',
        password=mssqlconfig['password']
    )

    sql1 = 'select distinct firstname,lastname,businessentityid from person.person '
    sql2 = 'select distinct addressline1,city,postalcode from person.address'
    sql3 = 'select stateprovinceid from person.stateprovince'
    sql4 = 'select addressid from person.address'
    sql5 = 'select customerid from sales.customer'
    sql6 = 'select productid, specialofferid from sales.specialofferproduct'

    with dbcon.cursor() as cursor:
        cursor.execute(sql1)
        row = cursor.fetchone()
        while row:
            fnames.append(row[0])
            lnames.append(row[1])
            personids.append(row[2])
            row = cursor.fetchone()
        cursor.execute(sql2)
        row = cursor.fetchone()
        while row:
            addresslines.append(row[0])
            cities.append(row[1])
            postalcodes.append(row[2])
            row = cursor.fetchone()

        cursor.execute(sql3)
        row = cursor.fetchone()
        while row:
            stateprovinceids.append(row[0])
            row = cursor.fetchone()

        cursor.execute(sql4)
        row = cursor.fetchone()
        while row:
            addressids.append(row[0])
            row = cursor.fetchone()

        cursor.execute(sql5)
        row = cursor.fetchone()
        while row:
            customerids.append(row[0])
            row = cursor.fetchone()

        cursor.execute(sql6)
        row = cursor.fetchone()
        while row:
            # special offer product id tuple as productid, specialofferid
            specialofferproductids.append(row)
            row = cursor.fetchone()


class Customer:
    def __init__(self, personid):
        self.personid = personid

    def toinsertsql(self) -> str:
        return f"""
        INSERT INTO SALES.CUSTOMER(PERSONID,TERRITORYID) VALUES ({self.personid},{random.randint(1,10)})
        """

    @ classmethod
    def createCustomer(cls):
        return Customer(personid=random.choice(personids))


class Person:
    def __init__(self,  firstname, middlename, lastname, persontype='sc', namestyle='0', title='',
                 suffix='', emailpromotion='1'):
        self.persontype = persontype
        self.namestyle = namestyle
        self.title = title
        self.firstname = firstname
        self.middlename = middlename
        self.lastname = lastname
        self.suffix = suffix
        self.emailpromotion = emailpromotion

    def toinsertsql(self) -> str:
        sql = f"""
        begin transaction;
        insert into person.BusinessEntity(modifieddate) values(getdate());
        INSERT INTO person.person(businessentityid,firstname,middlename,lastname,persontype,namestyle,title,suffix,emailpromotion)
            VALUES (scope_identity(), '{self.firstname.replace("'","''")}','{self.middlename}','{self.lastname.replace("'","''")}','{self.persontype}',{
                    self.namestyle},'{self.title}','{self.suffix}',{self.emailpromotion});
        commit;
        """
        return sql

    @ classmethod
    def createPerson(cls):
        return Person(firstname=random.choice(fnames),
                      lastname=random.choice(lnames),
                      middlename=''
                      )


class Address:
    def __init__(self, addressline1: str, city: str, stateprovinceid: str, postalcode: str):
        self.addressline1 = addressline1
        self.city = city
        self.stateprovinceid = stateprovinceid
        self.postalcode = postalcode

    def toinsertsql(self) -> str:
        sql = f"""INSERT INTO Person.Address(AddressLine1,City,StateProvinceId,PostalCode)
        values (
            '{self.addressline1.replace("'","''")}',
            '{self.city.replace("'","''")}',
            '{self.stateprovinceid}',
            '{self.postalcode}')
        """
        return sql

    @ classmethod
    def createAddress(cls):
        return Address(addressline1=random.choice(addresslines), city=random.choice(cities),
                       stateprovinceid=random.choice(stateprovinceids), postalcode=random.choice(postalcodes))


class SalesOrderHeader:
    def __init__(self):
        currenttime = datetime.now()
        self.orderdate = currenttime.strftime('%Y-%m-%d 00:00:00.000')
        self.duedate = (currenttime+timedelta(days=15)
                        ).strftime('%Y-%m-%d 00:00:00.000')
        self.shipdate = (currenttime+timedelta(days=7)
                         ).strftime('%Y-%m-%d 00:00:00.000')
        self.status = 5
        self.onlineorderflag = 0
        self.purchaseordernumber = f'PO{random.randint(1,10000000):08d}'
        self.accountnumber = f'10-{random.randint(1,9999):04d}-{random.randint(1,999999):06d}'
        self.customerid = random.choice(customerids)
        self.billtoaddressid = random.choice(addressids)
        self.shiptoaddressid = random.choice(addressids)
        self.territoryid = random.randint(1, 10)
        self.shipmethodid = 1
        self.subtotal = random.randint(1000, 5000)
        self.taxamt = self.subtotal * 0.07898
        self.freight = random.randint(200, 5000)
        self.revisionnumber = 8
        self.orderdetails = []
        for a in range(random.randint(1, 10)):
            self.orderdetails.append(SalesOrderDetail())

    def toinsertsql(self) -> str:
        sql = f"""
        INSERT INTO [Sales].[SalesOrderHeader]
           ([RevisionNumber]
           ,[OrderDate]
           ,[DueDate]
           ,[ShipDate]
           ,[Status]
           ,[OnlineOrderFlag]
           ,[PurchaseOrderNumber]
           ,[AccountNumber]
           ,[CustomerID]
           ,[TerritoryID]
           ,[BillToAddressID]
           ,[ShipToAddressID]
           ,[ShipMethodID]
           ,[SubTotal]
           ,[TaxAmt]
           ,[Freight])
     VALUES
           (
            {self.revisionnumber}
           ,'{self.orderdate}'
           ,'{self.duedate}'
           ,'{self.shipdate}'
           ,{self.status}
           ,{self.onlineorderflag}
           ,'{self.purchaseordernumber}'
           ,'{self.accountnumber}'
           ,{self.customerid}
           ,{self.territoryid}
           ,{self.billtoaddressid}
           ,{self.shiptoaddressid}
           ,{self.shipmethodid}
           ,{self.subtotal}
           ,{self.taxamt}
           ,{self.freight}
          );
        set @salesorderid = scope_identity();
        """
        for od in self.orderdetails:
            sql = sql + od.toinsertsql()
        return sql

    @classmethod
    def createSalesOrderHeader(cls):
        return SalesOrderHeader()


class SalesOrderDetail:
    def __init__(self):
        special = random.choice(specialofferproductids)
        self.orderqty = random.randint(1, 100)
        self.productid = special[0]
        self.specialofferid = special[1]
        self.unitprice = random.randint(2000, 60000)*1.0000 / 100
        self.unitpricediscount = round(random.random(), 2)

    def toinsertsql(self) -> str:
        sql = f"""
        INSERT INTO [Sales].[SalesOrderDetail]
           ([SalesOrderID]
           ,[OrderQty]
           ,[ProductID]
           ,[SpecialOfferID]
           ,[UnitPrice]
           ,[UnitPriceDiscount]
           )
        VALUES
           (
           @salesorderid 
           ,{self.orderqty}
           ,{self.productid}
           ,{self.specialofferid}
           ,{self.unitprice}
           ,{self.unitpricediscount}
           );       
        """
        return sql


prepareRandomDataLists()
fnames = list(set(fnames))
lnames = list(set(lnames))
personids = list(set(personids))
addresslines = list(set(addresslines))
cities = list(set(cities))
postalcodes = list(set(postalcodes))
