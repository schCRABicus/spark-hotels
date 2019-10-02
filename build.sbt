name := "HotelsRecommendationsSpark"

version := "0.1"

scalaVersion := "2.11.12"

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"

// https://mvnrepository.com/artifact/mrpowers/spark-daria
//libraryDependencies += "mrpowers" % "spark-daria" % "2.3.1_0.25.0"

// test dependencies
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
libraryDependencies += "MrPowers" % "spark-fast-tests" % "2.3.1_0.15.0" % "test"

excludeFilter in unmanagedResources := HiddenFileFilter || "*.csv*"
excludeFilter in managedResources := HiddenFileFilter || "*.csv*"
excludeFilter in resources := HiddenFileFilter || "*.csv*"