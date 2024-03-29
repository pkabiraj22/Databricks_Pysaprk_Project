# Databricks notebook source
/FileStore/tables/menu_csv.txt
/FileStore/tables/sales_csv.txt

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql.types import StructType, IntegerType, DoubleType, DateType, StringType, StructField

# COMMAND ----------

# DBTITLE 1,Creating Sales DF
schema = StructType([StructField("Product_ID", IntegerType(), True),
                    StructField("Customer_ID", StringType(), True),
                    StructField("Order_Date", DateType(), True),
                    StructField("Location", StringType(), True),
                    StructField("Source_Order", StringType(), True)
                    ]
)

Sales_df = spark.read.format('csv').option('inferschema',"True").schema(schema).load('/FileStore/tables/sales_csv.txt')
display(Sales_df)

# COMMAND ----------

# DBTITLE 1,Deriving Year, Month, Quarter
from pyspark.sql.functions import month, year, quarter



Sales_df = Sales_df.withColumn("Order_Year", year(Sales_df.Order_Date)) \
           .withColumn("Order_month", month(Sales_df.Order_Date)) \
           .withColumn("Order_Quarter", quarter(Sales_df.Order_Date))


# COMMAND ----------

display(Sales_df)

# COMMAND ----------

# DBTITLE 1,Creating Menu_Df
schema = StructType([StructField("Product_ID", IntegerType(), True),
                    StructField("Product_Name", StringType(), True),
                    StructField("Price", StringType(), True),
                    ]
)

Menu_df = spark.read.format('csv').option('inferschema',"True").schema(schema).load('/FileStore/tables/menu_csv.txt')
display(Menu_df)

# COMMAND ----------

# DBTITLE 1,Total Amount Spent by each customer
Total_amt_Cust = Sales_df.join(Menu_df,"Product_Id",how = "inner").groupBy("Customer_ID").agg({'Price':'sum'}).orderBy('Customer_Id')

display(Total_amt_Cust)

# COMMAND ----------

# DBTITLE 1,Total Amount Spent by each Food category
Total_amt_food = Sales_df.join(Menu_df,on= 'Product_Id', how = 'inner').groupBy('Product_Name').agg({'Price':'sum'}).orderBy('Product_Name')

display(Total_amt_food)

# COMMAND ----------

# DBTITLE 1, Monthly Sales

Total_Sales_Month = Sales_df.join(Menu_df, on = 'Product_Id', how = 'inner').groupBy('Order_month').agg({'Price':'sum'}).orderBy('Order_month')

display(Total_Sales_Month)

# COMMAND ----------

# DBTITLE 1,Yearly Sales
Total_Sales_Yearly = Sales_df.join(Menu_df, on = 'Product_Id', how = 'inner').groupBy('Order_Year').agg({'Price':'sum'}).orderBy('Order_Year')

display(Total_Sales_Yearly)

# COMMAND ----------

# DBTITLE 1,Quarterly Sales
Total_Sales_Quaterly = Sales_df.join(Menu_df, on = 'Product_Id', how = 'inner').groupBy('Order_Quarter').agg({'Price':'sum'}).orderBy('Order_Quarter')

display(Total_Sales_Quaterly)

# COMMAND ----------

# DBTITLE 1,Total no. of order by each Category
from pyspark.sql.functions import count

Total_order = Sales_df.join(Menu_df, on = 'Product_Id', how= 'inner').groupBy(['Product_Id','Product_Name']).agg(count('Product_Id').alias('Total_Orders')).orderBy('Total_Orders', ascending = 0)

display(Total_order)


# COMMAND ----------

# DBTITLE 1,Top 5 ordered items
from pyspark.sql.functions import count

Total_5_order = Sales_df.join(Menu_df, on = 'Product_Id', how= 'inner').groupBy(['Product_Id','Product_Name']).agg(count('Product_Id').alias('Total_Orders')).orderBy('Total_Orders', ascending = 0).limit(5)

display(Total_5_order)

# COMMAND ----------

# DBTITLE 1,Top ordered items
from pyspark.sql.functions import count

Top_ordered = Sales_df.join(Menu_df, on = 'Product_Id', how= 'inner').groupBy(['Product_Id','Product_Name']).agg(count('Product_Id').alias('Total_Orders')).orderBy('Total_Orders', ascending = 0).limit(1)

display(Top_ordered)

# COMMAND ----------

# DBTITLE 1,Frequency of Customer Visited
from pyspark.sql.functions import countDistinct

Freq_df = Sales_df.filter((Sales_df.Source_Order == 'Restaurant')).groupBy('Customer_Id').agg(countDistinct('Order_Date').alias('Frequency')).orderBy('Customer_Id')

display(Freq_df)

# COMMAND ----------

# DBTITLE 1,Total Sales by each country
Total_Sales_Country = Sales_df.join(Menu_df,on= 'Product_Id', how = 'inner').groupBy('Location').agg({'Price':'sum'}).orderBy('Location')

display(Total_Sales_Country)

# COMMAND ----------

# DBTITLE 1,Total Sales by order source
Total_Sales_Source = Sales_df.join(Menu_df,on= 'Product_Id', how = 'inner').groupBy('Source_Order').agg({'Price':'sum'}).orderBy('Source_Order')

display(Total_Sales_Source)

# COMMAND ----------


