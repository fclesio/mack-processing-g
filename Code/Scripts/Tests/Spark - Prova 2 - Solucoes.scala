


//1) Realize a carga do script do filme que foi indicado para voce em um RDD chamado scriptRDD_NOMEFILME
val scriptRDD_inception = sc.textFile("/home/mortar/Desktop/scripts/inception_script.txt")

val scriptRDD_schindler = sc.textFile("/home/mortar/Desktop/scripts/schindlers_list_script.txt")

val scriptRDD_godfather = sc.textFile("/home/mortar/Desktop/scripts/thegodfather_script.txt")



scriptRDD_inception

scriptRDD_schindler

scriptRDD_godfather


// 2) Realize a contagem do numero de objetos dentro do Dataframe
scriptRDD_inception.count
//res28: Long = 5667

scriptRDD_schindler.count
// res30: Long = 6552

scriptRDD_godfather.count
//res31: Long = 6792



// 3) Faca a contagem de palavras relativa ao seu respectivo filme
val palavras_inception = scriptRDD_inception.flatMap(l => l.split(" ")).map(word => (word,1)).reduceByKey(_ + _)

val inception_pivot = palavras_inception.map(_.swap)

val freq_inception = inception_pivot.sortByKey(false,1)

val top30_inception = freq_inception.take(30)
//top30_inception: Array[(Int, String)] = Array((1343,the), (690,to), (459,a), (445,COBB), (371,Cobb), (291,you), (289,and), (279,-), (271,of), (260,at), (258,his), (249,I), (232,in), (204,is), (186,INT.), (157,ARIADNE), (150,The), (145,looks), (144,Arthur), (140,on), (140,it), (134,ARTHUR), (129,into), (121,CONTINUOUS), (119,up), (117,He), (116,he), (114,we), (113,Ariadne), (113,for))



val palavras_schindler = scriptRDD_schindler.flatMap(l => l.split(" ")).map(word => (word,1)).reduceByKey(_ + _)

val schindler_pivot = palavras_schindler.map(_.swap)

val freq_schindler = schindler_pivot.sortByKey(false,1)

val top30_schindler = freq_schindler.take(30)
//top30_schindler: Array[(Int, String)] = Array((2195,the), (987,a), (947,-), (923,to), (879,of), (801,and), (439,his), (426,in), (332,SCHINDLER), (320,The), (293,Schindler), (287,on), (278,at), (260,I), (255,with), (253,is), (235,He), (232,from), (225,it), (213,you), (210,he), (209,for), (199,INT.), (184,DAY), (170,out), (156,EXT.), (146,as), (146,A), (139,are), (132,Goeth))



val palavras_godfather = scriptRDD_godfather.flatMap(l => l.split(" ")).map(word => (word,1)).reduceByKey(_ + _)

val godfather_pivot = palavras_godfather.map(_.swap)

val freq_godfather = godfather_pivot.sortByKey(false,1)

val top30_godfather = freq_godfather.take(30)
//top30_godfather: Array[(Int, String)] = Array((3430,""), (871,the), (590,to), (585,--), (411,a), (395,I), (386,and), (360,you), (229,in), (226,of), (224,MICHAEL), (208,TO:), (180,is), (159,CUT), (151,his), (138,(then,), (135,he), (130,with), (128,that), (121,my), (120,on), (118,your), (114,for), (111,as), (109,The), (104,TOM), (102,SONNY), (100,Michael), (98,be), (98,CORLEONE))

// 4) Faca a carga do arquivo housing.txt que esta no github
val data = sc.textFile("/home/mortar/Desktop/housing.txt")

// 5) Carregue um objeto que vai buscar os numeros do inFile
val dataRDD = data.map(line => line.split(','))

// 6) Faca a busca os 7 primeiros elementos dentro do RDD
dataRDD.take(7)

// 7) Usando a a propriedade flatMap separe cada elemento do arquivo data e insira em um RDD
val dataRDD = data.flatMap(line => line.split(',')).map(_.toDouble)

// 8) Use a propriedade collect para exibir o que tem dentro do RDD
dataRDD.collect()
//res53: Array[Double] = Array(0.00632, 18.0, 2.31, 0.0, 0.538, 6.575, 65.2, 4.09, 1.0, 296.0, 15.3, 396.9, 4.98, 24.0, 0.02731, 0.0, 7.07, 0.0, 0.469, 6.421, 78.9, 4.9671, 2.0, 242.0, 17.8, 396.9, 9.14, 21.6, 0.02729, 0.0, 7.07, 0.0, 0.469, 7.185, 61.1, 4.9671, 2.0, 242.0, 17.8, 392.83, 4.03, 34.7, 0.03237, 0.0, 2.18, 0.0, 0.458, 6.998, 45.8, 6.0622, 3.0, 222.0, 18.7, 394.63, 2.94, 33.4, 0.06905, 0.0, 2.18, 0.0, 0.458, 7.147, 54.2, 6.0622, 3.0, 222.0, 18.7, 396.9, 5.33, 36.2, 0.02985, 0.0, 2.18, 0.0, 0.458, 6.43, 58.7, 6.0622, 3.0, 222.0, 18.7, 394.12, 5.21, 28.7, 0.08829, 12.5, 7.87, 0.0, 0.524, 6.012, 66.6, 5.5605, 5.0, 311.0, 15.2, 395.6, 12.43, 22.9, 0.14455, 12.5, 7.87, 0.0, 0.524, 6.172, 96.1, 5.9505, 5.0, 311.0, 15.2, 396.9, 19.15, 27.1, 0.21124, 12.5, 7.87, 0.0, 0.524, 5.631, 100...
//sc

// 9)  Realize a soma dos objetos do RDD
dataRDD.sum()
//res54: Double = 472348.15522000124

// 10) Realize a contagem dos algarismos dentro do seu RDD 
dataRDD.count()
//res55: Long = 7084


//Objeto Key/Value para geracao de sequenceFile
val data = sc.parallelize(List((1,"Corinthians - SP" ,81,38,24,9,5,71,31,40,16,8,1,4,64,2,71),
(2,"Atlético - MG" ,69,38,21,6,11,65,47,18,13,8,4,7,76,4,60),
(3,"Grêmio - RS" ,68,38,20,8,10,52,32,20,14,6,2,8,95,2,59),
(4,"São Paulo - SP" ,62,38,18,8,12,53,47,6,12,6,1,11,73,5,54),
(5,"Internacional - RS" ,60,38,17,9,12,39,38,1,14,3,2,10,101,6,52),
(6,"Sport - PE" ,59,38,15,14,9,53,38,15,13,2,1,8,72,2,51),
(7,"Santos - SP" ,58,38,16,10,12,59,41,18,15,1,1,11,79,9,50),
(8,"Cruzeiro - MG" ,55,38,15,10,13,44,35,9,10,5,3,10,89,5,48),
(9,"Palmeiras - SP" ,53,38,15,8,15,60,51,9,9,6,6,9,91,5,46),
(10,"Atlético - PR" ,51,38,14,9,15,43,48,-5,9,5,4,11,87,7,44),
(11,"Ponte Preta - SP" ,51,38,13,12,13,41,40,1,9,4,6,7,99,5,44),
(12,"Flamengo - RJ" ,49,38,15,4,19,45,53,-8,8,7,8,11,82,6,42),
(13,"Fluminense - RJ" ,47,38,14,5,19,40,49,-9,10,4,6,13,98,10,41),
(14,"Chapecoense - SC" ,47,38,12,11,15,34,44,-10,9,3,5,10,84,3,41),
(15,"Coritiba - PR" ,44,38,11,11,16,31,42,-11,6,5,5,11,113,5,38),
(16,"Figueirense - SC" ,43,38,11,10,17,36,50,-14,7,4,6,11,111,5,37),
(17,"Avaí - SC" ,42,38,11,9,18,38,60,-22,8,3,6,12,119,3,36),
(18,"Vasco da Gama - RJ" ,41,38,10,11,17,28,54,-26,5,5,8,9,108,14,35),
(19,"Goiás - GO" ,38,38,10,8,20,39,49,-10,7,3,8,12,89,3,33),
(20,"Joinville - SC" ,31,38,7,10,21,26,48,-22,6,1,6,15,94,8,27)))

// 11) Faca a carga do arquivo Brasileiro 2015 dentro de um RDD
val data = sc.textFile("/home/mortar/Desktop/brasileiro_2015.csv")


// 12) Realize a gravacao do arquivo no seu desktop
data.saveAsTextFile("/home/mortar/Desktop/classificacao")


// 13) Realize o import das libs io.Text e a io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable


// 14) Realize a carga do objeto salvo no seu desktop
val result = sc.textFile("/home/mortar/Desktop/classificacao/part-00000")


// 15) Realize a exibicao dos dados desse novo objeto usando a propriedade collect()
result.collect


// 16) Abra uma nova sessao no Spark
import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder()
  .appName("Exemplos com Spark SQL")
  .config("spark.some.config.option", "1.1.0")
  .getOrCreate()

// 17) Insira a propriedade que faz conversao implicita  
import spark.implicits._

// 18) Crie um dataframe com o arquivo brasileiro_2015.json
val df = spark.read.json("/home/mortar/Desktop/brasileiro_2015.json")

df.show()

// 19) Realize a operacao de exibir o schema, e indique quais sao os datatypes
df.printSchema()

// 20) Selecione apenas a coluna time
df.select("Time").show()

// 21) Divida os pontos dos times por 3 para estimar o numero medio de vitorias desconsideranco outros fatores
df.select($"Time", $"P" / 3).show()

// 22) Filtre todos os times com mais de 100 cartoes amarelos
df.filter($"GA" >= 100).show()

// 23) Divida o numero de gols pro (GP) pelas 38 rodadas e indique o numero medio de gol por time 
df.select($"Time", $"GP" / 38).show()

// 24) Crie uma view temporaria com o conjunto de dados acima com o nome times em seguida insira em um objeto e exiba usando a opcao show()
df.createOrReplaceTempView("times")

val sqlDF = spark.sql("SELECT * FROM times")

sqlDF.show()

// 25) Faca o instanciamento de view global chamada time
df.createGlobalTempView("time")

// 26) Realize a consulta na view atrelado em uma base de dados no banco de dados 'global.temp'
spark.sql("SELECT * FROM global_temp.time").show()

// 27) Abrindo uma nova sessao realize a consulta na view global
spark.newSession().sql("SELECT * FROM global_temp.time").show()
