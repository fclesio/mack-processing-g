

// 1) Crie um RDD chamado discurso com o seu respectivo dataset indicado anteriormente usando o metodo textFile() Ex: Brasil sera discursos_BR Estados Unidos sera discursos_EUA

// 2) Realize a contagem no numero de itens dentro do seu RDD

// 3) Qual e a primeira linha do seu RDD?

// 4) Crie um RDD chamado deus_BR a partir do seu RDD discursos_BR e filtre a palavra "Deus" 
// 4) Crie um RDD chamado god_EUA a partir do seu RDD discursos_EUA e filtre a palavra "God"
//(Use a palavra da MESMA forma que esta descrita no exercicio, dado que o Scala e case-sensitive)

// 5) Crie um RDD chamado sou_BR a partir do seu RDD discursos_BR e filtre a palavra "Brasileiros" 
// 5) Crie um RDD chamado iam_EUA a partir do seu RDD discursos_EUA e filtre a palavra "Americans"
//(Use a palavra da MESMA forma que esta descrita no exercicio, dado que o Scala e case-sensitive)

// 6) Crie um RDD chamado brasil_BR a partir do seu RDD discursos_BR e filtre a palavra "Brasil" 
// 6) Crie um RDD chamado estadosunidos_EUA a partir do seu RDD discursos_EUA e filtre a palavra "America"
//(Use a palavra da MESMA forma que esta descrita no exercicio, dado que o Scala e case-sensitive)


// 7) Realize a contagem no numero de itens dentro dos seus RDDs
// Prova BR
deus_BR
sou_BR
brasil_BR

// Prova EUA
god_EUA
iam_EUA
estadosunidos_EUA

// 8) Crie uma funcao chamada func_equacao que realize a seguinte operacao: ((x + y) * w)/100
// a) x = 1, y = 10, w = 1000

// b) x = 10, y = 100, w = 10000

// c) x = 100, y = 1000, w = 100000


// 9) Apos criar a funcao do exercicio 8, passe os seguintes parametros e informe quais sao os resultados

// 10) Crie uma variavel chamada "dados" atraves de um array com 15 elementos que serao 2,4,6,8,10,12,14,16,18,20,22,24,26,28,30.


// 11) Usando a variavel dados, crie uma variavel chamada "dados_paralelos" que tenha como propriedade a funcao parallelize() que tenha 3 particoes


// 12) Depos de criar a variavel dados transforme ela em um RDD chamado dadosRDD


// 13) Pegue o seu arquivo inicial e salve como um arquivo de texto em seu sistema operacional na pasta Desktop. Se o seu exercicio estiver correto um arquivo do parquet sera criado na pasta
