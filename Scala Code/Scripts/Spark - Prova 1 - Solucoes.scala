// 1) Crie um RDD chamado discurso com o seu respectivo dataset indicado anteriormente usando o metodo textFile() Ex: Brasil sera discursos_BR Estados Unidos sera discursos_EUA

val discursos_BR = sc.textFile("/home/mortar/Desktop/discursos/discursos_brasil.txt")
//discursos_BR: org.apache.spark.rdd.RDD[String] = /Desktop/discursos/discursos_brasil.txt MapPartitionsRDD[1] at textFile at <console>:24

val discursos_EUA = sc.textFile("/home/mortar/Desktop/discursos/discursos_eua.txt")
//discursos_EUA: org.apache.spark.rdd.RDD[String] = /Desktop/discursos/discursos_eua.txt MapPartitionsRDD[3] at textFile at <console>:24

// 2) Realize a contagem no numero de itens dentro do seu RDD
discursos_BR.count()
//res2: Long = 19536

discursos_EUA.count()
//res0: Long = 2051

// 3) Qual e a primeira linha do seu RDD?
discursos_BR.first()
//res2: String = ""

discursos_EUA.first()
//res1: String = Fellow-Citizens:

// 4) Crie um RDD chamado deus_BR a partir do seu RDD discursos_BR e filtre a palavra "Deus" 
// 4) Crie um RDD chamado god_EUA a partir do seu RDD discursos_EUA e filtre a palavra "God"
//(Use a palavra da MESMA forma que esta descrita no exercicio, dado que o Scala e case-sensitive)

val deus_BR = discursos_BR.filter(line => line.contains("Deus"))
//deus_BR: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[4] at filter at <console>:27

val god_EUA = discursos_EUA.filter(line => line.contains("God"))
//god_EUA: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[5] at filter at <console>:29

// 5) Crie um RDD chamado sou_BR a partir do seu RDD discursos_BR e filtre a palavra "Brasileiros" 
// 5) Crie um RDD chamado iam_EUA a partir do seu RDD discursos_EUA e filtre a palavra "Americans"
//(Use a palavra da MESMA forma que esta descrita no exercicio, dado que o Scala e case-sensitive)

val sou_BR = discursos_BR.filter(line => line.contains("Brasileiros"))
//sou_BR: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[6] at filter at <console>:27

val iam_EUA = discursos_EUA.filter(line => line.contains("Americans"))
//iam_EUA: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[7] at filter at <console>:29

// 6) Crie um RDD chamado brasil_BR a partir do seu RDD discursos_BR e filtre a palavra "Brasil" 
// 6) Crie um RDD chamado estadosunidos_EUA a partir do seu RDD discursos_EUA e filtre a palavra "America"
//(Use a palavra da MESMA forma que esta descrita no exercicio, dado que o Scala e case-sensitive)

val brasil_BR = discursos_BR.filter(line => line.contains("Brasil"))
//brasil_BR: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[8] at filter at <console>:27

val estadosunidos_EUA = discursos_EUA.filter(line => line.contains("America"))
//estadosunidos_EUA: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[9] at filter at <console>:29

// 7) Realize a contagem no numero de itens dentro dos seus RDDs
// Prova BR
deus_BR
sou_BR
brasil_BR

// Prova EUA
god_EUA
iam_EUA
estadosunidos_EUA

//Solucao
deus_BR.count()
//res3: Long = 54

sou_BR.count()
//res2: Long = 14

brasil_BR.count()
//res1: Long = 484

god_EUA.count()
//res5: Long = 68

iam_EUA.count()
//res6: Long = 47

estadosunidos_EUA.count()
//res7: Long = 260


// 8) Crie uma funcao chamada func_equacao que realize a seguinte operacao: ((x + y) * w)/100

def fuc_equacao (x: Int, y: Int, w: Int) : Int = {return (((x + y)* w) / 100) }

// 9) Apos criar a funcao do exercicio 8, passe os seguintes parametros e informe quais sao os resultados

// a) x = 1, y = 10, w = 1000
fuc_equacao(1,10,1000)
//res8: Int = 110

// b) x = 10, y = 100, w = 10000
fuc_equacao(10,100,10000)
//res9: Int = 11000

// c) x = 100, y = 1000, w = 100000
fuc_equacao(100,1000,100000)
//res11: Int = 1100000


// 10) Crie uma variavel chamada "dados" atraves de um array com 15 elementos que serao 2,4,6,8,10,12,14,16,18,20,22,24,26,28,30.

val dados = Array(2,4,6,8,10,12,14,16,18,20,22,24,26,28,30)
//dados: Array[Int] = Array(2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30)


// 11) Usando a variavel dados, crie uma variavel chamada "dados_paralelos" que tenha como propriedade a funcao parallelize() que tenha 3 particoes

val dados_paralelos = sc.parallelize(dados,3)
//dados_paralelos: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[10] at parallelize at <console>:26


// 12) Depos de criar a variavel dados transforme ela em um RDD chamado dadosRDD

val dadosRDD = sc.makeRDD(dados)
//dadosRDD: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[11] at makeRDD at <console>:26

// 13) Pegue o seu arquivo inicial e salve como um arquivo de texto em seu sistema operacional na pasta Desktop. Se o seu exercicio estiver correto um arquivo do parquet sera criado na pasta
dadosRDD.saveAsTextFile("/home/mortar/Desktop/dados")
