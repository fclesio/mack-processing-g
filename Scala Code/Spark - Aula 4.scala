

//Abrindo uma sessão no Spark 
import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder()
  .appName("Exemplos com Spark SQL")
  .config("spark.some.config.option", "1.1.0")
  .getOrCreate()

// Na conversão de RDDs para Dataframes, esse comando realiza a conversão implicita. 
import spark.implicits._





//Criando Dataframes
val df = spark.read.json("examples/src/main/resources/people.json")

df.show()




//Operações com Dataframes
import spark.implicits._

df.printSchema()

//Selecionando apenas a coluna nome
df.select("name").show()

//Seleciona todos os elementos e aumenta a idade em um ano
df.select($"name", $"age" + 1).show()

//Filtra pessoas com mais de 21 anos
df.filter($"age" > 21).show()

//Agrupa via contagem todas as pessoas pela idade
df.groupBy("age").count().show()

//Referência de funções: http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset






//Programação via SQL no Scala


//Instanciamento de view temporária
df.createOrReplaceTempView("people")

val sqlDF = spark.sql("SELECT * FROM people")
sqlDF.show()

//Instanciamento de view global
df.createGlobalTempView("people")

// Consulta na view atrelado em uma base de dados no banco de dados 'global.temp'
spark.sql("SELECT * FROM global_temp.people").show()

// Com a consulta abaixo criamos uma sessão nova, e mesmo assim conseguimos acessar a view. 
spark.newSession().sql("SELECT * FROM global_temp.people").show()






//Criando Datasets no Scala

// Classe customizada pessoa
case class Person(name: String, age: Long)

// Usando encoders para criar classes case
val caseClassDS = Seq(Person("Andy", 32)).toDS()
caseClassDS.show()


// Com os enconders grande parte dos datatypes são parseados automáticamente devido ao comando importing spark.implicits._
val primitiveDS = Seq(1, 2, 3).toDS()
primitiveDS.map(_ + 1).collect() 

// Os DataFrames podem ser convertidos para um Dataset que tenha uma classe. 
val path = "examples/src/main/resources/people.json"
val peopleDS = spark.read.json(path).as[Person]
peopleDS.show()







//Interoperabilidade com os RDDs

//Inferência de esquema usando reflection
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder

// Comando para realizar conversões implicitas do RDD para Dataframe
import spark.implicits._

// Agora vamos criar um RDD do objeto person de um arquivo texto e converter isso para um dataframe
val peopleDF = spark.sparkContext
  .textFile("examples/src/main/resources/people.txt")
  .map(_.split(","))
  .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
  .toDF()

// Vamos registrar o DataFrame como uma view temporária
peopleDF.createOrReplaceTempView("people")

// Agora vamos criar um objeto no Scala usando uma consulta SQL em que vamos buscar todos os adolescentes
val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")

// As colunas na linha podem ser acessadas por um campo index
teenagersDF.map(teenager => "Name: " + teenager(0)).show()

// Ou pelo nome do campo
teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()

// Quando não há encoders predefinidos para o dataset baseado em Key-Value Dataset[Map[K,V]] esse comando faz a definição de forma explicita.
implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]

// Tipos primitivos e classes case podem ser definidas como 
implicit val stringIntMapEncoder: Encoder[Map[String, Any]] = ExpressionEncoder()

// O comando row.getValuesMap[T] busca múltiplas colunas e uma vez só denro de um mapa de features Map[String, T]
teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()






//Especificação manual do Schema
import org.apache.spark.sql.types._

// Vamos criar um RDD
val peopleRDD = spark.sparkContext.textFile("examples/src/main/resources/people.txt")

// No caso o schema está com o encoding string
val schemaString = "name age"

// Vamos gerar um schema baseado na string do schema
val fields = schemaString.split(" ")
  .map(fieldName => StructField(fieldName, StringType, nullable = true))
val schema = StructType(fields)

// Vamos converter os registros do RDD para linhas
val rowRDD = peopleRDD
  .map(_.split(","))
  .map(attributes => Row(attributes(0), attributes(1).trim))

// Vamos aplicar o schema para o RDD
val peopleDF = spark.createDataFrame(rowRDD, schema)

// Vamos criar uma view temporária usando Dataframe
peopleDF.createOrReplaceTempView("people")

// Nesse passo vamos executar uma consulta simples em uma view temporária usando Dataframes e armazenando na variável results
val results = spark.sql("SELECT name FROM people")

// As colunas da variável result pode ser acessada usando um índice
results.map(attributes => "Name: " + attributes(0)).show()
