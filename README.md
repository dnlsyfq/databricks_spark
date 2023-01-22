### Databricks Spark
```
spark
dbutils.help()
ls
dbutils.fs.ls("dbfs:/")
dbutils.fs.ls("dbfs:/databricks-datasets/")
dbutils.fs.ls("dbfs:/databricks-datasets/adult")


```

```
%fs

ls /databricks-datasets/adult/


```

```
adult_census_data = spark.read.csv("dbfs:/databricks-datasets/adult/adult.data",header=True)
type(adult_census_data)

adult_census_rdd = adult_census_data.rdd
type(adult_census_rdd)
```

```
adult_census_rdd.count()
adult_census_rdd.first()


adult_census_rdd.map(lambda row: (row[1], row[3], row[5])).collect()
adult_census_rdd.map(lambda row: (row[' State-gov'], row[' Adm-clerical'], row[' <=50K'])).collect()
adult_census_rdd_filtered = adult_census_rdd.filter(lambda row: row[' <=50K'] == ' <=50K')
adult_census_rdd_filtered.count()



dbutils.fs.ls("/databricks-datasets/bikeSharing/")
dbutils.fs.ls("/databricks-datasets/bikeSharing/data-001")

bike_sharing_data = spark.read.format("csv")\
    .option("inferSchema","True")\
    .option("header","True")\
    .option("sep",",")\
    .load("/databricks-datasets/bikeSharing/data-001/day.csv")
    
bike_sharing_data.show()    
```


```
bike_sharing_data_selected = bike_sharing_data.select('season','holiday','cnt')
bike_sharing_data_selected.show()
bike_sharing_data.filter(bike_sharing_data['cnt'] > 1000).show()
bike_sharing_data.filter(bike_sharing_data['yr'] == 0).count()

```


### Basic Transformations and Actions
```
df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/danialsyafiq@outlook.my/credit_train.csv")


credit_data = spark.read.format("csv")\
    .option("inferSchema","True")\
    .option("header","True")\
    .option("sep",",")\
    .load("dbfs:/FileStore/shared_uploads/danialsyafiq@outlook.my/credit_train.csv")
    
    
credit_data.count()
credit_data_subset = credit_data.select("Loan Status","Current Loan Amount","Annual Income")
credit_data.select("Loan Status","Current Loan Amount","Annual Income").limit(10).show()
credit_data.filter(credit_data['Annual Income'].isNull()).show()

credit_data.select("Customer ID","Annual Income","Home Ownership","Bankruptcies")\
    .where(credit_data.Bankruptcies > 0)\
    .show()
    
credit_data.select("Customer ID","Annual Income","Home Ownership","Tax Liens")\
    .filter((credit_data["Annual Income"]>100000) & (credit_data["Tax Liens"] > 0))\
    .show(10)
    
credit_data.select("Customer ID","Home Ownership")\
    .filter(credit_data["Home Ownership"].isin(["Home Mortgage","Rent"]))\
    .show(10)
    
    
credit_data.select("Customer ID","Annual Income","Monthly Debt")\
    .withColumnRenamed("Annual Income","Income")\
    .withColumnRenamed("Monthly Debt","Monthly Debt Payment")\
    .show(10)
    
credit_data.select("Customer ID","Annual Income","Monthly Debt")\
    .withColumn("Savings", credit_data["Annual Income"] - 12 * credit_data["Monthly Debt"])\
    .show(10)
    
credit_data.select("Customer ID","Annual Income","Monthly Debt")\
    .orderBy(credit_data["Monthly Debt"].desc())\
    .show(10)
    
credit_data.groupBy("Loan Status").count().show()
credit_data.groupBy("Purpose").agg({'Current Loan Amount':'sum'}).show()
credit_data.groupBy("Purpose").count().write.csv("dbfs:/FileStore/shared_uploads/danialsyafiq@outlook.my/count_by_loan_purpose.csv")


print(dbutils.fs.head('dbfs:/FileStore/shared_uploads/danialsyafiq@outlook.my/count_by_loan_purpose.csv/part-00000-tid-8302573433920391943-2b4dd093-523a-4c13-870f-45d4c184cc20-59-1-c000.csv'))






    
```

### Hive Metastore , Default Tables
```
auto_data =spark.sql('SELECT * FROM default.automobile_data')
display(auto_data)


auto_data = spark.sql('SELECT * FROM default.automobile_data')

auto_data_subset = spark.sql("SELECT MAKE, \
                             'body-style',\
                             price,\
                             horsepower\
                             FROM default.automobile_data\
                             WHERE price > 20000\
                             ")
                             
                             
from pyspark.sql.functions import initcap, upper, lower
from pyspark.sql.functions import translate
netflix_data = spark.sql('SELECT * FROM default.netflix_list')
display(netflix_data.select(upper('title'), lower('genres')))
display(netflix_data.select('title','type',initcap('type')))
display(netflix_data.select('title','origin_country',regexp_replace('language','En*','en')))

display(
    netflix_data.select('title',translate('title','io','10'))
)

netflix_data.createOrReplaceTempView('netflix_data_table')

```

```
people_data = spark.read.option('multiline',False)\
    .json('dbfs:/FileStore/tables/people.json')
iris_data = spark.read.option("multiline",True)\
    .json('dbfs:/FileStore/tables/iris.json')
employee_data = spark.read.option("multiline",True)\
    .option("mode","PERMISSIVE")\
    .json('dbfs:/FileStore/tables/employees.json')
    
    
display(people_data.filter(people_data.age >= 30))
display(iris_data.select('species').distinct())
display(employee_data.select("name","salary","address","contact"))
display(employee_data.select("name","salary","address.city","contact"))



from pyspark.sql import functions as F
display(employee_data.select(F.col('contact.email')
                             .getItem(0)
                             .alias('email_address')
                            ))
                            
 display(employee_data.select('name',
                             F.col('contact.email').getItem(0).alias('email_address'),
                             F.col('contact.phone').getItem(1).alias('phone_number')
                            ))
                            
                            
                            

```
