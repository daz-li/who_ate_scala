organization := "xxx.dazli"

name := "who-ate-scala-func"

version := "0.1-SNAPSHOT"

scalaVersion in ThisBuild := "2.10.4"

libraryDependencies ++= Seq(
    //"org.apache.spark" % "spark-core_2.11" % "1.1.0",
    "org.apache.spark" %% "spark-core" % "1.1.0",
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.3.1",
    "junit" % "junit" % "4.12" % "test",
    "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)

resolvers += Resolver.mavenLocal
