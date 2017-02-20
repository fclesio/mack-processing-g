0) Install java
sudo apt-get install default-jdk

1) Install JDK
sudo apt-get install default-jdk

2) Install htop
sudo apt-get install htop

3) Check Java
java -version

4) Install unzip
sudo apt-get install unzip 

5) Install Scala
http://www.scala-lang.org/download/2.11.6.html

or 

sudo apt-get install scala 

6) Check Scala version
scala -version

7) Caminho da instalação do Scala
which scala

8) Caminho da instalação do Java
which java

9) Caminho completo do Scala
whereis scala

10) Caminho completo do Java
whereis java

11) Instanciar o Scala
scala

12) Hello World em Scala (Usar o :paste antes para colar o código)
scala> object HelloWorld {
    |   def main(args: Array[String]): Unit = {
    |     println("Hello, world!")
    |   }
    | }


13) Print do objeto
scala> HelloWorld.main(Array())

14) Sair do Scala
scala>:q   

15) Instanciar o Shell do Spark
./bin/spark-shell

16) Check website padrão do Spark
10.0.2.15:4040/jobs
