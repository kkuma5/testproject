from pyspark.sql.functions import *
from pyspark.sql import SparkSession

source_file_location = 'raw_location'
spark = SparkSession.builder.appName("spark_raw_to_processed").getOrCreate()
src_df = spark.read.format("delta").parquet(source_file_location)
src_df.createOrReplaceTempView("src")

target_table_name = "customer_orders"

df = spark.sql("select * from src where load_date_time > select max(update_date) from {}".format(target_table_name))



df1 = df.select(
          "Customers.Customer.CompanyName",
          "Customers.Customer.ContactName",
          "Customers.Customer.ContactTitle",
          "Customers.Customer.Fax",
          "Customers.Customer.FullAddress.Address",
          "Customers.Customer.FullAddress.City",
          "Customers.Customer.FullAddress.Country",
          "Customers.Customer.FullAddress.PostalCode",
          "Customers.Customer.FullAddress.Region",
          "Customers.Customer.Phone",
          "Customers.Customer._CustomerID"
    ).selectExpr(
        "explode(arrays_zip(CompanyName, ContactName, ContactTitle, Fax, Address, City, Country, PostalCode, Region, Phone, _CustomerID)) col ",
    ).select("col.*")

df2 = df.select(
          "Orders.Order.CustomerID",
          "Orders.Order.EmployeeID",
          "Orders.Order.OrderDate",
          "Orders.Order.RequiredDate",
          "Orders.order.ShipInfo.Freight",
          "Orders.order.ShipInfo.ShipAddress",
          "Orders.order.ShipInfo.ShipCity",
          "Orders.order.ShipInfo.ShipName",
          "Orders.order.ShipInfo.ShipPostalCode",
          "Orders.order.ShipInfo.ShipRegion",
          "Orders.order.ShipInfo.ShipVia",
          "Orders.order.ShipInfo._ShippedDate"
    ).selectExpr(
        "explode(arrays_zip(CustomerID, EmployeeID, OrderDate, RequiredDate, Freight, ShipAddress, ShipCity, ShipName, ShipPostalCode, ShipRegion, ShipVia, _ShippedDate)) col ",
    ).select("col.*")

df3 = df2.join(df1, df2.CustomerID == df1._CustomerID, "outer")


df4 = df3.select("apply transformation logic and data type validation checks")


df4.registerTempTable("src")

spark.sql("""MERGE INTO customer_orders tgt
USING src
ON CustomerID = src.CustomerID
WHEN MATCHED THEN
  UPDATE SET
  CompanyName = src.CompanyName,
  ContactName = src.ContactName,
  ContactTitle = src.ContactTitle,
  Fax =  src.Fax,
  Address =  src.Address,
  City = src.City,
  Country = src.Country,
  PostalCode = src.PostalCode,
  Region = src.Region,
  Phone = src.Phone,
  OrderDate = src.OrderDate,
  RequiredDate = src.RequiredDate,
  Freight = src.Freight,
  ShipAddress =  src.ShipAddress,
  ShipCity = src.ShipCity,
  ShipName = src.ShipName,
  ShipPostalCode = src.ShipPostalCode,
  ShipRegion = src.ShipRegion,
  ShipVia = src.ShipVia,
  ShippedDate = src.ShippedDate,
  update_date = src.load_date_time
WHEN NOT MATCHED
  THEN INSERT (
    CustomerID,
    CompanyName,
    ContactName,
    ContactTitle,
    Fax,
    Address,
    City,
    Country,
    PostalCode,
    Region,
    Phone,
    OrderDate,
    RequiredDate,
    Freight,
    ShipAddress,
    ShipCity,
    ShipName,
    ShipPostalCode,
    ShipRegion,
    ShipVia,
    ShippedDate,
    insert_date,
    update_date
  )
  VALUES (
  src.CustomerID,
  src.CompanyName,
  src.ContactName,
  src.ContactTitle,
  src.Fax,
  src.Address,
  src.City,
  src.Country,
  src.PostalCode,
  src.Region,
  src.Phone,
  src.OrderDate,
  src.RequiredDate,
  src.Freight,
  src.ShipAddress,
  src.ShipCity,
  src.ShipName,
  src.ShipPostalCode,
  src.ShipRegion,
  src.ShipVia,
  src.ShippedDate,
  src.load_date_time,
  src.load_date_time"""
  )






