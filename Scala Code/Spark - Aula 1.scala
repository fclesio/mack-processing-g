// Ajuda do Scala
:help





// Cria um RDD chamado lines
val lines = sc.textFile("README.md") 

// Conta o número de itens dentro do lines
lines.count() 

// Pega a primeira linha do RDD
lines.first() 





// Filragem no Scala
val pythonLines = lines.filter(line => line.contains("Python"))

// Primeiro item da filtragem
pythonLines.first()

// Variavel que vai filtrar todas as linhas com a palavra Spark
val linesWithSpark = lines.filter(line => line.contains("Spark"))

// Contagem de quantas linhas tem a palavra spark
lines.filter(line => line.contains("Spark")).count() 

// Contagem das palavras
lines.map(line => line.split(" ").size).reduce((a, b) => if (a > b) a else b)





//Aplicativo Word Count (Contador de palavras)

//Imports do Java
import java.lang.Math

// Mapeamento das linhas e divisão pelo espaço
lines.map(line => line.split(" ").size).reduce((a, b) => Math.max(a, b))

// Transforma em pares e realiza a contagem
val wordCounts = lines.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)

// Mostra o array com todas as combinações das palavras e as suas frequencias
wordCounts.collect()

// Quantidade de linhas com a palavra Spark
linesWithSpark.cache()

// Quantidade de linhas com a palavra Spark
linesWithSpark.count()






// Preparar o console para um comando de copiar e colar
:paste

// Loading do arquivo var.txt, e contagem das palavras do arquivo
val textFile = sc.textFile("/home/snipper/Desktop/var.txt")

val counts = textFile.flatMap(line => line.split(" "))
                 .map(word => (word, 1))
                 .reduceByKey(_ + _)
counts.saveAsTextFile("/home/snipper/Desktop/var_count")

// Cria um dataframe que tem uma única coluna chamada line
val df = textFile.toDF("line")

// Checar o dataframe
df

// Variavel que tem o filtro da palavra VAR
val valueatrisk = df.filter(col("line").like("%VAR%"))

// Contagem de todas as palavras VAR
valueatrisk.count()

// Contagem de todas as menções da palavra RISK
valueatrisk.filter(col("line").like("%risk%")).count()

// Mostra o array das palavras relacionadas com a palavra RISK
valueatrisk.filter(col("line").like("%risk%")).collect()





// Declara a variavel tup1 com o valor 0 e o nome zero
val tup1 = (0, "zero")

// Informa as propriedades da variável
tup1.getClass

// Declara a variavel tup1 com o valor 1, o nome zero e um valor 1
val tup2 = (1, "um", 1.0)

// Informa as propriedades da variável
tup2.getClass

// Checagem da tupla 1 com a primeira propriedade
println(tup1._1)

// Checagem da tupla 1 com a segunda propriedade
println(tup1._2)

// Checagem da tupla 2 com a segunda propriedade
scala> println(tup2._2)

// Checagem da tupla 2 com a terceira propriedade
scala> println(tup2._3)





// Declaração de uma função
def sum (x: Int, y: Int) : Int = { return x + y }

//Passagem de parâmetros para a função e a execução
sum (1,2)




// Função de maximo
val max: (Int, Int) => Int = (m: Int, n: Int) => if(m > n) m else n

// Retorno da função 
max(3,4)

// Funcao de maximo
def max (x: Int, y: Int) : Int = { if (x > y) x else y } 

// Array com valores
val data = Array(1, 2, 3, 4, 5)

// Cria uma coleção de elementos que serão paralelizados. Depois de criado esses elementos podem ser operados em paralelo
val distData = sc.parallelize(data)

//Checagem da variavel data
data

// Paralelizar o array com 10 partições
val distData = sc.parallelize(data, 10) 

// Pega a coleção de elementos, e transorma em um RDD
val dataRDD = sc.makeRDD(data)

// Junção dos dados
distData.reduce((a, b) => a + b) 

// Geração do arquivo distFile com o dataset
val distFile = sc.textFile("/home/snipper/Desktop/var.txt")

// Número de palavras
distFile.map(s => s.length).reduce((a, b) => a + b) 





// Arquivar o texto em uma variável
val file = sc.textFile("/home/snipper/Desktop/var.txt")

//Contagem
file.count

// Salvar como objeto
file.saveAsObjectFile("/tmp/readme_object")

// Salvar como arquivo de texto
file.saveAsTextFile("/tmp/readme_text")


//Ir no bash e executar esses dois scripts para ver o conteúdo dos arquivos gerados
// bash> cat /tmp/readme_object/part-00000
// bash> cat /tmp/readme_text/part-00000

// Para ver os arquivos gerados na pasta
// bash>  ls /tmp/readme_object/
// bash>  ls /tmp/readme_text/


// Para reagrupar os arquivos basta reagrupar todas as informações da pasta
val f = sc.textFile("/tmp/readme_text/*")

// Podemos contar novamente
f.count

//e para restaurar o nosso objeto
val f = sc.objectFile("/tmp/readme_object/*")






//Initializing Spark in Java
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
JavaSparkContext sc = new JavaSparkContext(conf);

//Initializing Spark in Python
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)



// Links e referências
// http://www.inf.ufpr.br/erlfilho/tutorials/spark/
// https://www.infoq.com/br/articles/apache-spark-introduction
// http://spark.apache.org/examples.html
// https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/SparkKMeans.scala
// http://www.fooledbyrandomness.com/jorion.html
// http://www.tutorialspoint.com/apache_spark/
// https://spark.apache.org/docs/2.1.0/spark-standalone.html
// https://spark.apache.org/docs/2.1.0/quick-start.html
// http://spark.apache.org/downloads.html
// https://spark.apache.org/docs/2.1.0/

