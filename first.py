import os
import urllib.request
import ssl

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

hadoop_home = os.path.abspath("hadoop")   # <-- absolute path
os.makedirs(os.path.join(hadoop_home, "bin"), exist_ok=True)

urls_and_paths = {
    "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/master/test.txt":os.path.join(data_dir, "test.txt"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/winutils.exe":os.path.join(hadoop_home, "bin", "winutils.exe"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/hadoop.dll":os.path.join(hadoop_home, "bin", "hadoop.dll")
}

# Create an unverified SSL context
ssl_context = ssl._create_unverified_context()

for url, path in urls_and_paths.items():
    # Use the unverified context with urlopen
    with urllib.request.urlopen(url, context=ssl_context) as response, open(path, 'wb') as out_file:
        data = response.read()
        out_file.write(data)
import os, urllib.request, ssl; ssl_context = ssl._create_unverified_context(); [open(path, 'wb').write(urllib.request.urlopen(url, context=ssl_context).read()) for url, path in { "https://github.com/saiadityaus1/test1/raw/main/df.csv": "df.csv", "https://github.com/saiadityaus1/test1/raw/main/df1.csv": "df1.csv", "https://github.com/saiadityaus1/test1/raw/main/dt.txt": "dt.txt", "https://github.com/saiadityaus1/test1/raw/main/file1.txt": "file1.txt", "https://github.com/saiadityaus1/test1/raw/main/file2.txt": "file2.txt", "https://github.com/saiadityaus1/test1/raw/main/file3.txt": "file3.txt", "https://github.com/saiadityaus1/test1/raw/main/file4.json": "file4.json", "https://github.com/saiadityaus1/test1/raw/main/file5.parquet": "file5.parquet", "https://github.com/saiadityaus1/test1/raw/main/file6": "file6", "https://github.com/saiadityaus1/test1/raw/main/prod.csv": "prod.csv", "https://raw.githubusercontent.com/saiadityaus1/test1/refs/heads/main/state.txt": "state.txt", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv", "https://github.com/saiadityaus1/SparkCore1/raw/refs/heads/master/data.orc": "data.orc", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv", "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/refs/heads/master/rm.json": "rm.json"}.items()]

# ======================================================================================

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
import os
import urllib.request
import ssl

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['HADOOP_HOME'] = hadoop_home
os.environ['JAVA_HOME'] = r'C:\Users\Moham\.jdks\corretto-1.8.0_502'        #  <----- 🔴JAVA PATH🔴
######################🔴🔴🔴################################

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.12:3.5.4 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 pyspark-shell'


conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

spark.read.format("csv").load("data/test.txt").toDF("Success").show(20, False)


##################🔴🔴🔴🔴🔴🔴 -> DON'T TOUCH ABOVE CODE -- TYPE BELOW ####################################

from pyspark.sql.functions import *

print("started")


# csvfile = (
#     spark
#     .read
#     .format("csv")
#     .option("header", "true")
#     .load("usdata.csv")
# )
# print("====CSV====")
# csvfile.show()
#
# parquetfile = (
#     spark.
#     read.
#     format("parquet")
#     .load("file5.parquet")
# )
# print("====Parquet====")
# parquetfile.show()

# jsonfile = (
#     spark
#     .read
#     .format("json")
#     .load("file4.json")
# )
# print("====JSON====")
# jsonfile.show()

# orcfile = (
#     spark
#     .read
#     .format("orc")
#     .load("data.orc")
# )
# print("====ORC====")
# orcfile.show()

# jsonfile.createOrReplaceTempView("success")
# spark.sql('''
#         SELECT AMOUNT,SPENDBY,CASE WHEN SPENDBY = "cash" THEN 1 ELSE 0 END AS STATUS
#         FROM success
#         ''').show()
# spark.sql("""
#         SELECT AMOUNT,CATEGORY,CONCAT(AMOUNT,"-",CATEGORY) as CONCATED
#         FROM SUCCESS
#
#         """).show()
#
# spark.sql('''
#         SELECT CATEGORY,SPENDBY,ROW_NUMBER() OVER
#         (PARTITION BY CATEGORY ORDER BY CATEGORY DESC)  AS ROWNUMBER
#         FROM SUCCESS
#         ''').show()


# data = [
#     (1, "Kaif", "IT", 50000, "2023-01-10"),
#     (2, "Rahul", "IT", 60000, "2022-05-15"),
#     (3, "Aman", "IT", 55000, "2023-03-20"),
#     (4, "Sahana", "HR", 45000, "2022-08-10"),
#     (5, "Priya", "HR", 52000, "2023-02-15"),
#     (6, "Ravi", "HR", 52000, "2021-11-05"),
#     (7, "John", "Finance", 70000, "2022-04-12"),
#     (8, "Sara", "Finance", 65000, "2023-06-18"),
#     (9, "Mike", "Finance", 70000, "2021-09-25"),
#     (10, "Anil", "IT", 60000, "2021-07-30")
# ]
#
# columns = [
#     "id",
#     "name",
#     "department",
#     "salary",
#     "joining_date"
# ]
#
# df = spark.createDataFrame(data, columns)
#
# df.show()
# #
# df.createOrReplaceTempView("my_table")
#
# spark.sql("""
#     SELECT DEPARTMENT,SALARY,LAG(SALARY) OVER (PARTITION BY DEPARTMENT
#     ORDER BY SALARY DESC) as row_number
#     FROM my_table
#
#     """).show()

# dsldataframe = csvfile.filter("state = 'LA'")
# dsldataframe.show()

# dslselect = df.select("name","department")
# dslselect.show()
#
# dsldrop = df.drop("department","salary")
# dsldrop.show()

# dslcondition = df.filter("department = 'HR'")
# dslcondition.show()
#
# dslmulticol = df.filter("department = 'HR' and salary > 45000")
# dslmulticol.show()
#
# dslmulcol =df.filter( "department = 'HR' or salary > 60000")
# dslmulcol.show()
#
# dslmulvalue = df.filter("department in ('HR','IT')")
# dslmulvalue.show()

# dsllikevalue = df.filter("name like 'A%'")
# dsllikevalue.show()

# dslexpr = df.selectExpr(
#     "id",
#     "upper(name) as NAME",
#     "concat(department, '~zeyo') as department",
#     "(salary + 1000) as salary",
#     "split(joining_date,'-')[0] as year",
#     "case when department = 'IT' then 1 when  department = 'HR' then 2 else 3 end as status "
# )
# dslexpr.show()


#####################################WITH COLUMN################################################
# dslwc = (
#     df.withColumn("name" , expr("upper(name)"))
#     .withColumn("department" , expr("concat(department, '~zeyo')"))
#     .withColumn("salary",expr("salary + 1000"))
#     .withColumn("status", expr("""
#         case when department = 'IT~zeyo' then 1
#              when department = 'HR~zeyo' then 2
#              else 3 end
#     """))
#     .withColumn("joining_date", expr("split(joining_date,'-')[0]"))
#     .withColumnRenamed("joining_date","year")
# )
# dslwc.show()

######################################JOINS#########################################################
# employee_data = [
#     (1, "Kaif", 101, 50000),
#     (2, "Rahul", 102, 60000),
#     (3, "Sahana", 101, 55000),
#     (4, "Anjali", 103, 65000),
#     (5, "Ravi", 104, 48000),
#     (6, "John", 105, 70000)
# ]
#
# employee_columns = [
#     "emp_id",
#     "emp_name",
#     "dept_id",
#     "salary"
# ]
#
# employees_df = spark.createDataFrame(
#     employee_data,
#     employee_columns
# )
#
# employees_df.show()
#
# department_data = [
#     (101, "IT", "Bangalore"),
#     (102, "HR", "Mumbai"),
#     (103, "Finance", "Delhi"),
#     (104, "Sales", "Chennai"),
#     (106, "Marketing", "Hyderabad")
# ]
#
# department_columns = [
#     "dept_id",
#     "department",
#     "location"
# ]
#
# departments_df = spark.createDataFrame(
#     department_data,
#     department_columns
# )
#
# departments_df.show()

###############################INNER JOIN########################################################
# innerj = employees_df.join(departments_df ,employees_df["emp_dept_id"] == departments_df["dept_id"],"inner")
# innerj.show()

###############################LEFT JOIN######################################################
# leftj = employees_df.join(departments_df , ["dept_id"], "left")
# leftj.show()

###############################RIGHT JOIN######################################################
# rightj = employees_df.join(departments_df, ["dept_id"], "right")
# rightj.show()

###############################FULL JOIN######################################################
# fulljoin = employees_df.join(departments_df, ["dept_id"], "leftanti")
# fulljoin.show()

# finaljoin = ( fulljoin
#     .withColumn("dept_id" , expr("case when dept_id is null then emp_dept_id else dept_id end"))
#     .withColumn("new_dept_id" , expr("coalesce(dept_id,emp_dept_id)")).drop("emp_dept_id")
# )
# finaljoin.show()

# listdata = employees_df.select("dept_id").rdd.flatMap( lambda x : x ).collect()
# print(listdata)
#
# compare = departments_df.filter(~col("dept_id").isin(listdata))
# compare.show()
#
# crossj = employees_df.crossJoin(departments_df).orderBy("emp_name").show()



#########################################SECINARIO'S ############################################
# source_data = [
#     (1, "A"),
#     (2, "B"),
#     (3, "C"),
#     (4, "D")
# ]
#
# source_input = spark.createDataFrame(
#     source_data,
#     ["id", "name"]
# )
#
# source_input.show()
#
# target_data = [
#     (1, "A"),
#     (2, "B"),
#     (4, "X"),
#     (5, "F")
# ]
#
# target_input = spark.createDataFrame(
#     target_data,
#     ["id", "name1"]
# )
#
# target_input.show()
#
# output = source_input.join(target_input , ["id"] , "full" )
# output.show()
#
#
# final = output.withColumn( "comment" , expr("""
#             case
#                 when name = name1 then 1 else 0
#             end
#     """))
# final.show()
#
# fil = final.filter("comment != 1")
# fil.show()
#
# finalop = fil.withColumn("comment" , expr("""
#             case
#                 when name is null then "new in target"
#                 when name1 is null then "new in source"
#                 else "mismatch"
#             end
#     """))
# finalop.show()

###########################AGGREGATIONS##########################################################
# data = [
#     ("sai", 40, "chn"),
#     ("zeyo", 10, "hyd"),
#     ("sai", 20, "hyd"),
#     ("zeyo", 20, "chn"),
#     ("sai", 10, "chn"),
#     ("zeyo", 10, "hyd")
# ]
#
# columns = ["name", "amount", "city"]
#
# df = spark.createDataFrame(data, columns)
#
# df.show()
#
# aggre = df.groupBy("name","city").agg(
#     sum("amount").alias("total"),
#     count("name").alias("cnt"),
#     collect_list("amount").alias("list"),
#     collect_set("amount").alias("set")
# )
# aggre.show()

# data = [('2020-05-30','Headphone'),('2020-06-01','Pencil'),('2020-06-02','Mask'),('2020-05-30','Basketball'),('2020-06-01','Book'),('2020-06-02','Mask'),('2020-05-30','T-Shirt')]
# columns = ["sell_date",'product']
#
# df = spark.createDataFrame(data,schema=columns)
# df.show()
#
# aggdf = df.dropDuplicates().groupBy("sell_date").agg(
#         collect_set("product").alias("product"),
#         count("product").alias("null_sell")
# )
# aggdf.show()

####################################WINDOW FUNCTIONS###################################################################
from pyspark.sql.window import Window
# data = [
#     ("DEPT1", 1000),
#     ("DEPT1", 500),
#     ("DEPT1", 700),
#     ("DEPT2", 400),
#     ("DEPT2", 200),
#     ("DEPT3", 200),
#     ("DEPT3", 500)
# ]
#
# columns = ["department", "salary"]
#
# df = spark.createDataFrame(data, columns)
#
# df.show()
#
# dfwindow = Window.partitionBy("department").orderBy(col("salary").desc())
#
# dataf = df.withColumn("rnk" , dense_rank().over(dfwindow))
#
# dataf.filter("rnk = 2").drop("rnk").show()

##################################COMPLEX DATA#########################################################################

# jsondata = spark.read.format("json").option("multiline", "true").load("d.json")
# jdata = jsondata.selectExpr(
#     "id",
#     "name",
#     "zeyoaddress.user.permanentaddress",
#     "zeyoaddress.user.Temparoryaddress"
# )
# jdata.show()
# jdata.printSchema()

################################### REVISION #####################################################

# list = [1 , 4 , 6 , 7]
# print(list)
# rdd = sc.parallelize(list)
# final = rdd.map( lambda x:x + 2)
# print(final.collect())
#
# list = ["zeyobron", "zeyo" , "analytics"]
# print(list)
# rdd = sc.parallelize(list)
# final = rdd.filter( lambda x : "zeyo" in x)
# print(final.collect())

###################################### column based rdd filtering ###############################
#
# from collections import namedtuple
# data = sc.textFile("file1.txt")
# # data.foreach(print)
#
# mapdata = data.map(lambda x : x.split(","))
#
#
#
# column = namedtuple('column' , ['txno','txndate','custno','amount','category','product','city','state','spendby'])
#
# fin = mapdata.map(lambda x : column(x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8]) )
#
# fi = fin.filter(lambda x : "Gymnastic" in x.product)
# fi.foreach(print)

#####################################converting rdd to df #################################################

# df = fi.toDF()
# df.show()

# readcsv = spark.read.format("csv").option("header","true").load("file3.txt")
# print("CSV")
# readcsv.show(5)
# print(readcsv.count())
#
# readjson = spark.read.format("json").load("file4.json").select('txnno','txndate','custno','amount','category','product','city','state','spendby')
# print("JSON")
# readjson.show(5)
# print(readjson.count())
#
# print("Union")
# uniondf = readcsv.union(readjson)
# uniondf.show(5)
# print(uniondf.count())
#
# wtdf = (uniondf.withColumn("txndate", expr("split(txndate,'-')[2]"))
#         .withColumnRenamed("txndate","year")
#         .withColumn("status" , expr("""
#             case
#             when "spendby" = "cash" then 1 else 0
#             end
#         """))
#
#
#         )
# wtdf.filter("txnno > 50000").show(20)
#
# csdf = wtdf.groupBy("category").agg(sum("amount").alias('total'),
#                                     count('custno').alias('cnt')
#                                     )
# csdf.show(10)

# windf = Window.partitionBy('department').orderBy(col('salary').desc())
#
# wind = df.withColumn("rank", dense_rank().over(windf)).filter("rank = 2")
# wind.show()


###################################### Complex data struct ##########################################
# data = [
#     """
#     {
#         "place": "Hyderabad",
#         "student" : [
#             "akashya",
#             "kaif"
#         ]
#     }
#     """
# ]
#
# df = spark.read.json(sc.parallelize(data))
#
# df.show()
#
# df.printSchema()
# flatdata = df.selectExpr(
#
#     "place",
#     "user.name",
#     "user.address.number",
#     "user.address.street",
#     "user.address.pin"
# )
# flatdata.show()
# flatdata.printSchema()

# data = [
#     '''
#     {
#         "age": 25,
#         "billing_address": {
#             "address": "502, Main Market",
#             "city": "Vasai Road, Palghar",
#             "postal_code": "401208",
#             "state": "Maharashtra"
#         },
#         "date_of_birth": "2001-05-15",
#         "email_address": "zeyo@gmail.com",
#         "first_name": "Zeyo",
#         "height_cm": 185.2,
#         "is_alive": true,
#         "last_name": "Khan",
#         "shipping_address": {
#             "address": "Ezeelive Technologies",
#             "city": "Mumbai",
#             "postal_code": "400058",
#             "state": "Maharashtra"
#         }
#     }
#     ''',
#
#     '''
#     {
#         "age": 30,
#         "billing_address": {
#             "address": "MG Road",
#             "city": "Bangalore",
#             "postal_code": "560001",
#             "state": "Karnataka"
#         },
#         "date_of_birth": "1996-08-20",
#         "email_address": "sai@gmail.com",
#         "first_name": "Sai",
#         "height_cm": 175.5,
#         "is_alive": true,
#         "last_name": "Kumar",
#         "shipping_address": {
#             "address": "Whitefield",
#             "city": "Bangalore",
#             "postal_code": "560066",
#             "state": "Karnataka"
#         }
#     }
#     '''
# ]
#
# rdd = sc.parallelize(data)
#
# df = spark.read.json(rdd)
#
# df.show()
#
# df.printSchema()
#
# flatdata = (
#     df.withColumn("billing_add" , expr("billing_address.address"))
#     .withColumn("billing_city" , expr("billing_address.city"))
#     .withColumn("billing_post" , expr("billing_address.postal_code"))
#     .withColumn("billing_state" , expr("billing_address.state"))
#     .drop('billing_address')
# )
# flatdata.show()
# flatdata.printSchema()

###################################### Complex data Array ##########################################
# arrdf = (
#     df.selectExpr(
#         "place",
#         "explode(student) as students"
#     )
# )
# arrdf.show()

# data = [
#     '''
#     {
#       "Actors": [
#         {
#           "Birthdate": "1975-03-15",
#           "BornAt": "New York",
#           "age": 49,
#           "hasChildren": true,
#           "hasGreyHair": false,
#           "name": "John Smith",
#           "photo": "john.jpg",
#           "picture": {
#             "large": "john_large.jpg",
#             "medium": "john_medium.jpg",
#             "thumbnail": "john_thumb.jpg"
#           },
#           "weight": 75.5,
#           "wife": "Jane Smith"
#         },
#         {
#           "Birthdate": "1980-07-20",
#           "BornAt": "Los Angeles",
#           "age": 44,
#           "hasChildren": true,
#           "hasGreyHair": true,
#           "name": "Robert Brown",
#           "photo": "robert.jpg",
#           "picture": {
#             "large": "robert_large.jpg",
#             "medium": "robert_medium.jpg",
#             "thumbnail": "robert_thumb.jpg"
#           },
#           "weight": 82.3,
#           "wife": "Emily Brown"
#         }
#       ],
#       "country": "USA",
#       "version": "1.0"
#     }
#     '''
# ]
# rdd = sc.parallelize(data)
# df = spark.read.json(rdd)
# df.show()
# df.printSchema()
#
# dumdf = df.withColumn("Actors" , expr("explode(Actors)"))
# dumdf.show()
# dumdf.printSchema()
#
# finaldf = dumdf.selectExpr(
#     "Actors.Birthdate",
#     "Actors.BornAt",
#     "Actors.age",
#     "Actors.hasChildren",
#     "Actors.hasGreyHair",
#     "Actors.name",
#     "Actors.photo",
#     "Actors.picture.large",
#     "Actors.picture.medium",
#     "Actors.picture.thumbnail",
#     "Actors.weight",
#     "Actors.wife",
#     "country",
#     "version",
# )
#
# finaldf.show()
# finaldf.printSchema()
