name := "DataQualityAutomation"

version := "0.0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"
libraryDependencies += "com.amazon.deequ" % "deequ" % "1.0.2"
libraryDependencies += "com.microsoft.azure" % "azure-storage" % "3.1.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-azure" % "2.7.3"
libraryDependencies += "org.rogach" %% "scallop" % "3.3.1"
