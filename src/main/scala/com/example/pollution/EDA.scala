package com.example.pollution

import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._

object EDA {
  
    
    /**
     * Signification des fonctions d'analyse exploratoire des données (EDA)
     */

    /**
      * Pollution moyenne par heure
      */
    /**
      * ici df est le DataFrame final que l'on a nettoyé et préparé dans Main.scala
      * .groupBy permet de regrouper toutes les lignes qui ont la meme heure
      * .agg est une aggregation . En gros on dit à Spark ; pour chaque groupe d'heure, calcule des statistiques
      * .avg("pollution") calcule la moyenne de la colonne pollution pour chaque groupe d'heure
      * .alias("avg_pollution") renomme la colonne résultante en avg_pollution
      * .orderBy("hour") trie les résultats par heure croissante
      */

    def pollutionParHeure(df:DataFrame): DataFrame= 
        df.groupBy("hour")
            .agg(avg("pollution").alias("avg_pollution"))
            .orderBy("hour")

    /**
      * Pollution moyenne par mois
      */


    /**
      *Objectif : Voir s'il ya des saisons de forte pollution
      *Hiver ?chauffage
      *été ? pollution par l'ozone à cause du trafic routier
      *autonne?
      */


    
    def pollutionParMois (df:DataFrame): DataFrame=
        df.groupBy("month")
            .agg (avg ("pollution").alias("avg_pollution"))
            .orderBy("month")
    /**
      * Pollution moyenne par jour de la semaine
      */     
    def pollutionParJourSemaine (df:DataFrame): DataFrame=
        df.groupBy("dayofweek")
            .agg (avg ("pollution").alias("avg_pollution"))
            .orderBy("dayofweek")
    
    /**
      * Statistiques globales de la pollution (min, max, moyenne)
      
      */

    /** 
     * min("pollution") calcule la valeur minimale de la colonne pollution
     * max("pollution") calcule la valeur maximale de la colonne pollution
     * avg("pollution") calcule la valeur moyenne de la colonne pollution
    */
    def statsPollution (df:DataFrame): DataFrame=
        df.agg(
            max("pollution").alias("max_pollution"),
            min("pollution").alias("min_pollution"),
            avg("pollution").alias("avg_pollution")
        )
    /**
      * Corrélation entre pollution et température
      */

    /** 
     * calcule la correlation de Pearon entre les colonnes pollution et température
     * la correlation mesure la force et la direction de la relation linéaire entre deux variables
     * La correlation varie entre -1 et 1
     * -1 : correlation négative parfaite (quand une variable augmente, l'autre diminue)
     * 0 : pas de correlation (les variables sont indépendantes)
     * 1 : correlation positive parfaite (quand une variable augmente, l'autre augmente aussi)
    */
    def correlationPollutionTemperature (df:DataFrame): Double=
        df.stat.corr("pollution","temp")
    




    /**Analyse APPROFONDIE DES DONNÉES 

    Statistiques Journalières de la pollution
    
    On extrait  les dates (sans l'heure )pou regrouper par journée
    */

    def dailyPollutionStats (df:DataFrame): DataFrame={
        val dfWithDate = df.withColumn("date_only", to_date(col("timestamp")))
    
        dfWithDate.groupBy("date_only")
            .agg(
                max("pollution").alias("max_pollution"),
                min("pollution").alias("min_pollution"),
                avg("pollution").alias("avg_pollution")
            )
            .orderBy("date_only")
    }


    /**
     * Top des N journées les plus polluées
     */
     
     def topPollutedDays(df: DataFrame, n: Int = 10): DataFrame = {
        val daily = dailyPollutionStats(df)
        daily.orderBy(col("avg_pollution").desc).limit(n)
    }

     /** 
      * Repartition des niveaux de pollution (Low, Medium, High)
      * Nous utiliserons la colonne pollution_level créée dans Main.scala
      */
     def pollutionLevelDistribution(df:DataFrame):DataFrame={
        val total = df.count().toDouble
        df.groupBy("pollution_level")
        .agg(count("*").alias("count"))
        .withColumn("percentage",col("count")/total *100)
        .orderBy("pollution_level")
     }


     /** 
      * Par de pollution "High" par mois 
     */
    def highPollutionByMonth(df:DataFrame):DataFrame={
        val grouped = df.groupBy("year","month")
            .agg(
                count("*").alias("total_count"),
                sum(when(col("pollution_level") === "High",1).otherwise(0)).alias("high_count")
            )
        grouped 
        .withColumn("high_ratio", col("high_count") / col("total_count"))
        .orderBy("year", "month")
    }
   
      /* Fonction utilitaire pour tout lancer et afficher les résultats de l'EDA
      */
      def runEDA(df:DataFrame): Unit = {
        println("Pollution moyenne par heure :")
        pollutionParHeure (df).show(24, truncate = false)

        println("Pollution moyenne par mois :")
        pollutionParMois (df).show(12, truncate = false)

        println("Pollution moyenne par jour de la semaine :")
        pollutionParJourSemaine (df).show(7, truncate = false)

        println("Statistiques globales de la pollution :")
        statsPollution (df).show(truncate = false)

        val corr = correlationPollutionTemperature (df)
        println(s"Correlation entre pollution et temperature : $corr")

        //Analyse approfondie :
            
        println("Statistiques journalieres de la pollution")
        dailyPollutionStats(df).show(10, truncate = false)

        println(" Top 10 journees les plus polluees (moyenne) ")
        topPollutedDays(df, 10).show(10, truncate = false)

        println(" Repartition des niveaux de pollution (Low / Medium / High) ")
        pollutionLevelDistribution(df).show(truncate = false)

        println(" Part de pollution 'High' par mois ")
        highPollutionByMonth(df).show(24, truncate = false)
  
      }



}