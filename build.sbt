name := "twitter4"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.0"

libraryDependencies += "org.apache.spark" %% "spark-unsafe" % "1.5.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.5.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.5.0"

//addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.0")
//
//libraryDependencies += "com.eed3si9" %% "sbt-assembly" % "0.14.0"
