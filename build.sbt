ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "spark-pollution",
    version := "0.1.0",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core"   % "3.5.1",
      "org.apache.spark" %% "spark-sql"    % "3.5.1",
      "org.apache.spark" %% "spark-mllib"  % "3.5.1",
      "org.apache.spark" %% "spark-graphx" % "3.5.1",
      "org.knowm.xchart" %  "xchart"       % "3.8.1",
      // "org.graphstream" % "gs-core" % "2.0",
      // "org.graphstream" % "gs-ui" % "2.0"
    )
  )


