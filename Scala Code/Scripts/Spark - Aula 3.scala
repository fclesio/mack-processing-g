
/* Primeiro load de arquivos */
val inFile = sc.textFile("./spam.data")




/* Instanciamento do SparkFiles para buscar dados de inumeras fontes dentro de um determinado filesystem*/
import org.apache.spark.SparkFiles

val file = sc.addFile("/home/mortar/Desktop/spam.data")

val inFile = sc.textFile(SparkFiles.get("spam.data"))





/* Divisao das linhas de acordo com o espaco */
val nums = inFile.map(line => line.split(' ').map(_.toDouble))

/* Checagem dos primeiros elementos do objeto infile */
inFile.first()

/* Checagem dos primeiros elementos do objeto nums*/
nums.first()






/*Spark Shell usando Python - Neste caso temos que chamar na pasta bin o pyspark*/

/*Leitura do arquivo README*/
textFile = sc.textFile("README.md")

/*Contagem de elementos*/
textFile.count()

/*Primeiros elementos*/
textFile.first()

/*Quantidade de linhas com a palavra spark usando o filter do textFile*/
//linesWithSpark = textFile.filter(lambda line: "Spark" in line)

/*Filtro puro*/
//textFile.filter(lambda line: "Spark" in line).count()




//Carga de Dados
val dataRDD = sc.parallelize(List(1,2,4))

//Busca 3 elementos dentro do RDD
dataRDD.take(3)




//Trabalhando com elementos numericos
val inFile = sc.textFile("/home/mortar/Desktop/List_of_numbers.csv")

//Objeto que vai buscar os numeros do inFile
val numbersRDD = inFile.map(line => line.split(','))

//Busca os 10 primeiros elementos
numbersRDD.take(10)


val numbersRDD = inFile.map(line => line.split(',')).map(_ .toDouble)

val numbersRDD = inFile.map(line => line.split(',')).map(_.toDouble)


//Trabalhando com a propriedade flatMap
val numbersRDD = inFile.flatMap(line => line.split(',')).map(_.toDouble)

//Objetos do RDD
numbersRDD.collect()

//Soma dos objetos do RDD
numbersRDD.sum()



//Trabalhando com arquivos CSV
import au.com.bytecode.opencsv.CSVReader
import java.io.StringReader

//Carga de dados
sc.addFile("/home/mortar/Desktop/List_of_numbers.csv")

//Carga no objeto
val inFile = sc.textFile("/home/mortar/Desktop/List_of_numbers.csv")

//Vai realizar a divisao de linhas e vai fazer a leitura 
val splitLines = inFile.map(line => {
val reader = new CSVReader(new StringReader(line))
reader.readNext()
})

//Dados numericos e conversao para double
val numericData = splitLines.map(line => line.map(_.toDouble))

//Soma dos dados
val summedData = numericData.map(row => row.sum)

//Apresentacao da soma
println(summedData.collect().mkString(","))




//Sequence file? (I do not think so)
val data = sc.sequenceFile[String, Int]("/home/mortar/Desktop/List_of_numbers.csv")




//Salvando dados no Scala
val data = sc.textFile("/home/mortar/Desktop/spam.data")

//Salvando como arquivo texto
data.saveAsTextFile("/home/mortar/Desktop/Teste.txt")





//Objeto Key/Value para geracao de sequenceFile
val data = sc.parallelize(List(("Luis", 1), ("Carmen", 2), ("Gilmar", 2)))

//Conteudo do arquivo 
data.first()

//Gravacao do arquivo
data.saveAsSequenceFile("/home/mortar/Desktop/seqoutput")






//Leitura dos dados de sequence
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable

//Apontamento do arquivo com os dados de sequence
val result = sc.sequenceFile("/home/mortar/Desktop/seqoutput/part-00000", classOf[Text], classOf[IntWritable]). map{case (x, y) => (x.toString, y.get())}

//Exibicao dos dados
result.collect







/* Carga de dados via objeto no Amazon S3*/
val file = sc.textFile("s3n://buckect/pasta/subpasta/")

/* Checagem do arquivo*/
file.first()

/*Checagem do primeiro elemento*/
file.take(1)


/*Mapeamento com a chave hardcoded*/
s3n://<AWS ACCESS ID>:<AWS SECRET>@bucket/path.

/*Salvar objecto no S3*/
sample.saveAsTextFile("s3n://mysparkbucket/test")
