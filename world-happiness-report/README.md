# <center style="color:Blue">World Happiness Dataset with Spark</center>

<center><img src="https://steemitimages.com/1280x0/https://www.psychologies.co.uk/sites/default/files/styles/psy2_page_header/public/wp-content/uploads/2012/03/happy.jpg"></center>

# <center>Importing spark and data</center>
      !pip install pyspark #install pySpark

      import pyspark
      from pyspark.sql import SparkSession
      spark= SparkSession.builder.getOrCreate()
      
      path= "DataSet/world-happiness-report.csv"
      data= spark.read.csv(path)
      data.show(10)

<img width="1151" alt="1" src="https://user-images.githubusercontent.com/51120437/127311735-30259dc5-4289-4262-826d-8df859a32402.png">

      data.printSchema()
      
<img width="1151" alt="1" src="https://user-images.githubusercontent.com/51120437/127311831-80fc92c9-d12c-44a0-9a5e-1a5499c7de40.png">

## 1.1 Creat a schema 

      from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
      new_schema= StructType([StructField("Country", StringType(), True),
                              StructField("Year", StringType(), True),
                              StructField("Life_Ladder", StringType(), True),
                              StructField("Log_Gdp", StringType(), True),
                              StructField("Social_support", StringType(), True),
                              StructField("Healthy_life_expectancy", StringType(), True),
                              StructField("Freedom_to_make_life_choices ", StringType(), True),
                              StructField("Generosity", StringType(), True),
                              StructField("Perceptions_of_corruption", StringType(), True),
                              StructField("Positive_affect", StringType(), True),
                              StructField("Negative_affect", StringType(), True)
                             ])
                             
                             
      data= spark.read.csv(path, schema=new_schema)
      data.show(3)

<img width="1149" alt="1" src="https://user-images.githubusercontent.com/51120437/127311984-1bc1d37f-ee92-4d3f-ae39-7623bdce4852.png">


## 1.2 Changing datatype

      for i in data.columns:
          if i != "Country":
              data= data.withColumn(i+"_",data[i].cast(FloatType())).drop(i)
      data= data.withColumn('Year',data['Year_'].cast(IntegerType())).drop("Year_")
      
      data.printSchema()
      
<img width="965" alt="1" src="https://user-images.githubusercontent.com/51120437/127312081-a55d0561-445c-4770-8997-546bbddfbb61.png">

      data.show(5)
      
<img width="1145" alt="1" src="https://user-images.githubusercontent.com/51120437/127312241-dedb3b3d-12c0-49bd-83fa-dfbcb34d7fe5.png">
## 1.3 Drop null values
      
      print(data.count())
      data=data.na.drop()
      data.show(5)
      print(data.count())

<img width="1145" alt="1" src="https://user-images.githubusercontent.com/51120437/127312373-0b155a87-2684-4719-8d52-8db9d1878cca.png">

# <center> Data Exploration </center>
      
## Question1: How many Country there are?
      country_data = data.groupBy('Country').count().show()
      
<img width="1132" alt="1" src="https://user-images.githubusercontent.com/51120437/127312512-a0a9d828-f1d6-4657-8279-cc768498c640.png">

## Question2: How many data rows collected from Afghanistan?

      Af_data= data.filter(data['Country']=="Afghanistan")
      Af_data= Af_data.drop('Country')
      print("Number of rows that collected from Afghanistan =", Af_data.count())
      Af_data.show(12)
      
<img width="1143" alt="1" src="https://user-images.githubusercontent.com/51120437/127312592-6fd1ef91-8151-4705-b990-5a8c2acb85b7.png">

## Question3: what is tha average of Positive and Negative affect in Afghanistan?

      Af_data.groupBy().avg('Positive_affect_').show()
      Af_data.groupBy().avg('Negative_affect_').show()
<img width="1149" alt="1" src="https://user-images.githubusercontent.com/51120437/127312682-c74ef266-3175-43e5-8008-19b8dc802dc2.png">

# <center>Adding 2021 Dataset to the main data</center>

      path_2021= "DataSet/world-happiness-report-2021.csv"
      data_2021= spark.read.csv(path_2021, schema=new_schema)
      data_2021.show(3)      
<img width="1161" alt="1" src="https://user-images.githubusercontent.com/51120437/127312791-551886bc-88a7-4b77-b38b-419138528810.png">

      for i in data_2021.columns:
          if i != "Country":
              data_2021= data_2021.withColumn(i+"_",data_2021[i].cast(FloatType())).drop(i)
      data_2021= data_2021.withColumn('Year',data_2021['Year_'].cast(IntegerType())).drop("Year_")
      data_2021.show(10)
<img width="1062" alt="1" src="https://user-images.githubusercontent.com/51120437/127312865-cd7b4713-4364-4156-bb5a-e08386620d4a.png">

      all_data= data.union(data_2021)
      all_data.show(5)

<img width="1063" alt="1" src="https://user-images.githubusercontent.com/51120437/127312929-4aead636-6cfa-4a88-9b6b-9184ca90183d.png">














                             
