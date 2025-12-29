package com.example.pollution

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._
import org.knowm.xchart.{XYChartBuilder, SwingWrapper}
import org.knowm.xchart.style.Styler.LegendPosition
import scala.jdk.CollectionConverters._
import scala.io.StdIn
import org.knowm.xchart.{XYChartBuilder, CategoryChartBuilder, SwingWrapper}
import org.apache.hadoop.shaded.com.google.common.graph.Graph
import java.awt.{Desktop => allMetrics}




object Main {
  def main(args: Array[String]): Unit = {

   /* creation de la session spark */
    val spark=SparkSession.builder().appName("Projet Pollution Spark")
      .master("local[*]") //on exécute en local avec tous les coeurs
      .getOrCreate()

    //pour reduire  le bruit des logs
    spark.sparkContext.setLogLevel("WARN")

    /* test de vérification */
    println("spark est bien lance")

    //lescture des données kaggle 
    val inputPath="data/input/LSTM-Multivariate_pollution.csv"
    
    val dfRaw=spark.read
      .option("header","true") //premiere ligne =nom de colonnes 
      .option("inferSchema","true") //spark devine les types 
      .csv(inputPath)

      println("aperçu des donnees")
      dfRaw.show(10,truncate = false)


      //nettoyage des données 

      //retirons toutes les lignes vides 

      val dfClean=dfRaw.na.drop()
      println(s"nombre de lignes avant nettoyage : ${dfRaw.count()}")
      println(s"nombre de lignes apres nettoyage : ${dfClean.count()}")
      println("aperçu des donnees nettoyees")
      dfClean.show(10,truncate = false)

      //suppressions des doublons
      val dfNoDupli =dfClean.dropDuplicates()
      println(s"nombre de lignes apres suppression des doublons : ${dfNoDupli.count()}")

      //remplacer les valeurs manquantes 
      val dfImpute = dfNoDupli.na.fill(Map(
            "temp" -> 0,
            "dew" -> 0,
            "wnd_spd" -> 0,
            "pollution" -> 0
          ))
      println("aperçu des donnees apres imputation")
      dfImpute.show(10,truncate = false)



      //convertissons la colonne date en timestamp
      val dfTS = dfImpute.withColumn("timestamp",to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss"))
      println("aperçu des donnees avec date en timestamp")
      dfTS.show(10,truncate = false)

      //extraire les variables temporelles : année, mois, jour, heure

      val dfTimes= dfTS
        .withColumn("year",year(col("timestamp")))
        .withColumn("month",month(col("timestamp")))
        .withColumn("day",dayofmonth(col("timestamp")))
        .withColumn("hour",hour(col("timestamp")))
        .withColumn("dayofweek",date_format(col("timestamp"),"E"))
      println("apperçu des donnees avec variables temporelles")
      dfTimes.show(10,truncate = false) 

      val dfFinal = dfTimes.withColumn(
          "pollution_level",
          when(col("pollution") < 50, "Low")
            .when(col("pollution") < 100, "Medium")
            .otherwise("High")
        )

      

      val outputPath="data/processed/pollution_clean.parquet"
      //sauvegarde au format parquet
      dfFinal.write.mode("overwrite").parquet(outputPath)
      println(s"donnees nettoyees sauvegardees dans : $outputPath")


    //Analyse exploratoire des données (EDA)
    println("Lancement de l'EDA")
    EDA.runEDA(dfFinal)

    val pollutionHour = dfFinal
      .groupBy("hour")
      .agg(avg("pollution").alias("avg_pollution"))
      .orderBy("hour")
      .collect()

    // ---- Graphique pollution moyenne par heure ----

    


    // ---- Graphique pollution moyenne par heure ----
    val pollutionPerHour = dfFinal
      .groupBy("hour")
      .agg(avg("pollution").alias("avg_pollution"))
      .orderBy("hour")
      .collect()

    val hours    = pollutionPerHour.map(_.getInt(0)).map(Int.box).toList.asJava
    val averages = pollutionPerHour.map(_.getDouble(1)).map(Double.box).toList.asJava

    val chart = new XYChartBuilder()
      .width(800)
      .height(600)
      .title("Pollution moyenne par heure")
      .xAxisTitle("Heure")
      .yAxisTitle("Pollution moyenne")
      .build()


    chart.addSeries("Pollution", hours, averages)
    new SwingWrapper(chart).displayChart()

    
    println("Graphique 1 : appuie sur Entree pour continuer…")
    StdIn.readLine()

    // ---- Graphique pollution moyenne par mois ----
    val pollutionPerMonth = dfFinal
      .groupBy("month")
      .agg(avg("pollution").alias("avg_pollution"))
      .orderBy("month")
      .collect()

    val months = pollutionPerMonth.map(_.getInt(0)).map(Int.box).toList.asJava
    val avgByMonth = pollutionPerMonth.map(_.getDouble(1)).map(Double.box).toList.asJava

    val chart2 = new XYChartBuilder()
      .width(800)
      .height(600)
      .title("Pollution moyenne par mois")
      .xAxisTitle("Mois")
      .yAxisTitle("Pollution")
      .build()

    chart2.addSeries("Pollution", months, avgByMonth)
    new SwingWrapper(chart2).displayChart()
    println("Graphique 2 : appuie sur Entree pour continuer…")
    StdIn.readLine()

    // ---- Graphique pollution moyenne par jour de la semaine ----
    val pollutionPerDay = dfFinal
      .groupBy("dayofweek")
      .agg(avg("pollution").alias("avg_pollution"))
      .orderBy("dayofweek")
      .collect()

    val dayNames = pollutionPerDay.map(_.getString(0)).toList.asJava
    val avgByDay = pollutionPerDay.map(_.getDouble(1)).map(Double.box).toList.asJava

    // CategoryChart car l'axe X contient des catégories (Mon, Tue, ...)
    val chart3 = new CategoryChartBuilder()
      .width(800)
      .height(600)
      .title("Pollution moyenne par jour de la semaine")
      .xAxisTitle("Jour")
      .yAxisTitle("Pollution")
      .build()

    chart3.addSeries("Pollution", dayNames, avgByDay)

    new SwingWrapper(chart3).displayChart()
    println("Graphique 3 : appuie sur Entree pour terminer")
    StdIn.readLine()


    println("Lancement de l'analyse graphe (GraphX)")
    GraphAnalysis.runGraph(dfFinal)

    println(" Etape ML 1 : Regression lineaire seule ")
    MLAnalysis.runLinearOnly(dfFinal)

    println("Etape ML 2 : Random Forest seul ")
    MLAnalysis.runRandomForestOnly(dfFinal)

    println("Etape ML 3 : GBT seul ")
    MLAnalysis.runGBTOnly(dfFinal)

    println("Etape ML 4 : Comparaison globale sur meme split ")
    MLAnalysis.runAllModels(dfFinal)
  

    spark.stop()
  }
}
