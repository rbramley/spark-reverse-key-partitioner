name         := "spark-reverse-key-partitioner"
version      := "0.1.0"
organization := "com.rbramley"

scalaVersion := "2.11.12"
//crossScalaVersions := Seq("2.11.12","2.12.10")

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3" % "provided"

libraryDependencies += "junit" % "junit" % "4.10" % Test
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test

resolvers += Resolver.mavenLocal
resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

bintrayPackageLabels := Seq("spark", "partitioning")
bintrayVcsUrl := Some("git@github.com:rbramley/spark-reverse-key-partitioner.git")
