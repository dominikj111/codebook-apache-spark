
name := "MovieSimilarities1M"
organization := "com.sundogsoftware"
version := "1.0"

scalaVersion := "2.11.12"

val sparkVersion = "2.2.1"

libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // "org.apache.spark" %% "spark-mllib" % sparkVersion,
  // "org.apache.spark" %% "spark-streaming" % sparkVersion,
  // "org.apache.spark" %% "spark-streaming-kafka" % sparkVersion,
  // "org.apache.spark" %% "spark-hive" % sparkVersion,
  // "org.apache.spark" %% "spark-streaming-twitter" % sparkVersion,
  // "org.apache.spark" %% "spark-streaming-flume" % sparkVersion,
  // "org.apache.spark" %% "spark-mllib" % sparkVersion,

  // "org.apache.commons" % "commons-csv" % "1.5",

  "org.slf4j" % "slf4j-api" % "1.7.25",
  "org.slf4j" % "slf4j-log4j12" % "1.7.25",
  "org.clapper" %% "grizzled-slf4j" % "1.3.2",

  "com.beachape" %% "enumeratum-circe" % "1.5.15"
)

dependencyOverrides ++= Seq(
  "io.netty" % "netty" % "3.9.9.Final",
  "commons-net" % "commons-net" % "3.1",
  "com.google.guava" % "guava" % "16.0.1"
)

// resolvers ++= Seq(
//   "apache-snapshots" at "http://repository.apache.org/snapshots/",
//   "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
//   "Spray Repository" at "http://repo.spray.cc/",
//   "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
//   "Akka Repository" at "http://repo.akka.io/releases/",
//   "Twitter4J Repository" at "http://twitter4j.org/maven2/",
//   "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
//   "Twitter Maven Repo" at "http://maven.twttr.com/",
//   "scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools",
//   "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
//   "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
//   "Mesosphere Public Repository" at "http://downloads.mesosphere.io/maven",
//   Resolver.sonatypeRepo("public")
// )

// resolvers ++= Seq(
//   "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
// )
