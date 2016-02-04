//import AssemblyKeys._

name := "Simple Project"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.5.1" %  "provided"

libraryDependencies += "org.apache.spark" % "spark-graphx_2.10" % "1.5.2" % "provided"

libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.9"

//libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-mapping" % "2.1.5"
//libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.8"
libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.10" % "1.5.0-M2"

//libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector-java_2.10" % "1.5.0-M2"

//assemblySettings

jarName in assembly :="my-project-assembly.jar"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)


resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
    {
        case PathList("netty", "handler", xs @ _*)         => MergeStrategy.first
        case PathList("netty", "buffer", xs @ _*)     => MergeStrategy.first
        case PathList("netty", "common", xs @ _*)     => MergeStrategy.first
        case PathList("netty", "transport", xs @ _*)     => MergeStrategy.first
        case PathList("netty", "codec", xs @ _*)     => MergeStrategy.first
        //case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
        case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
        case x => old(x)
        }
    }