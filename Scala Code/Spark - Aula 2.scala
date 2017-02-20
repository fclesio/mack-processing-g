//Spark - Aula 2

// Básico de RDD

// Criando um RDD de strings com a função textFile()
var lines = sc.textFile("README.md")

// Chamada da transformação filter()
pythonLines = lines.filter(line => line.contains("Python"))

// Chamada da Ação first()
pythonLines.first()

// Persistindo o RDD na memória
pythonLines.persist

// Contagem de linhas 
pythonLines.count()

// Leitura da primeira frase
pythonLines.first()





// Paralizando em Scala
val lines = sc.parallelize(List("pandas", "i like pandas"))

// Método textFile() Scala
val lines = sc.textFile("/path/to/README.md")





// Operações com RDD

// Transformações (Transformations)
// Operações em um RDD que retornam um novo RDD

// Criação da variável que faz a carga do texto dos logs
val inputRDD = sc.textFile("log.txt")

// Criação da variável que faz a carga somente das linhas que contém a palavra ERROR
val errorsRDD = inputRDD.filter(line => line.contains("error"))

// Criação da variável que faz a carga somente das linhas que contém a palavra WARNING
val warningsRDD = inputRDD.filter(line => line.contains("warning"))

// Aqui usamos a transformation UNION() para unir as mensagens
val badLinesRDD = errorsRDD.union(warningsRDD)





// Ações (Actions)
// Ações são operações no RDD para fazer alguma atividade com o RDD, isso é
// retornam um valor final para escrita de dados.  

// Nesse caso vamos imprimir op número de linhas com erro usando a ação count()
println("Input had " + badLinesRDD.count() + " concerning lines")

//Aqui vamos printar 10 exemplos do erros usando o método take() e foreach()
println("Here are 10 examples:")
badLinesRDD.take(10).foreach(println)

// Toda vez que chamamos uma ação, todo o conjunto de dados é lido
// desde o ínicio, e para evitar isso o ideal é que haja uma persistência desses registros 





// Vamos usar esse snippet abaixo para realizar uma função multiplicativa de cada elemento por si mesmo para usar com as funções de caching.
val input = sc.parallelize(List(1, 2, 3, 4))
val result = input.map(x => x * x)
println(result.collect().mkString(","))





// Persistência (Caching)

// Vamos criar uma variável com o método map() (Método que aplica uma função para cada elemento do RDD e retorna o resultado em um RDD)
val result = input.map(x => x*x)

// Aqui vamos imprimir o número de elemetos
println(result.count())

// E aqui vamos mostrar os valores dos elementos
println(result.collect().mkString(","))

// Até aqui já processamos todas as vezes os elementos de dados, mas agora vamos persistir isso em disco para que a execução fique mais velozß
// No caso, os dados aqui ficarão hosteados em nó, que se cair, o Spark refaz o cálculo






// Para persistir, primeiramente vamos importar o StorageLevel do Spark 
import org.apache.spark.storage.StorageLevel

// Vamos usar o mesmo método map() para fazer o cálculo no RDD
val result = input.map(x => x * x)

// Para persistir somente na memória, usamos a função abaixo. (Espaço: Alto, CPU: Baixo, Na memória: S, No disco: N)
result.persist(StorageLevel.MEMORY_ONLY)

// Para persistir somente na memória, usamos a função abaixo. (Espaço: Baixo, CPU: Alto, Na memória: S, No disco: N)
result.persist(StorageLevel.MEMORY_ONLY_SER)

// Para persistir somente no disco, usamos a função abaixo. (Espaço: Baixo, CPU: Alto, Na memória: N, No disco: S)
result.persist(StorageLevel.DISK_ONLY)

// Para persistir no disco e na memória em conjunto, usamos a função abaixo. (Espaço: Alto, CPU: Médio, Na memória: S, No disco: S) Move para o disco se houver pouco espaço na memória.
result.persist(StorageLevel.MEMORY_AND_DISK)

// Para persistir somente no disco, usamos a função abaixo. (Espaço: Baixo, CPU: Alto)
result.persist(StorageLevel.MEMORY_AND_DISK_SER)




// Vamos imprimir a contagem
println(result.count())

// Agora a contagem com os resultados
println(result.collect().mkString(","))

// Retirar o cache
result.unpersist() 