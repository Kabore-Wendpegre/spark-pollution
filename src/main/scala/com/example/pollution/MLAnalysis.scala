package com.example.pollution

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression._
import org.apache.spark.ml.evaluation.RegressionEvaluator

import org.apache.spark.ml.linalg.Vector

// Pour les graphes XChart
import org.knowm.xchart.{XYChartBuilder, CategoryChartBuilder, SwingWrapper}
import scala.jdk.CollectionConverters._

import scala.io.StdIn

/**
  * Module de prédiction de la pollution avec Spark MLlib.
  *
  * - Prépare les features
  * - Entraîne 3 modèles : Linéaire, RandomForest, GBT
  * - Compare leurs performances (RMSE, R²)
  * - Génère des graphiques de comparaison (XChart)
  */
object MLAnalysis {

  /** Noms des features utilisés pour la prédiction */
  val featureCols: Array[String] = Array(
    "temp", "dew", "press", "wnd_spd", "snow", "rain",
    "hour", "month"
  )

  /** Structure pour stocker les métriques d'un modèle */
  case class ModelMetrics(
    name: String,
    rmse: Double,
    r2: Double
  )


  // 1) Préparation des features


  /**
    * Prépare les données pour les modèles ML :
    *   - Crée la colonne "label" (pollution)
    *   - Crée la colonne "features" (Vector des colonnes explicatives)
    */
  def prepareFeatures(df: DataFrame): DataFrame = {
    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")

    val dfWithLabel = df.withColumn("label", col("pollution").cast("double"))

    assembler.transform(dfWithLabel)
  }

  /** Renvoie deux évaluateurs (RMSE et R²) réutilisables. */
  private def buildEvaluators(): (RegressionEvaluator, RegressionEvaluator) = {
    val evaluatorRmse = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val evaluatorR2 = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("r2")

    (evaluatorRmse, evaluatorR2)
  }


  // 2) Fonctions d'entraînement "briques de base"
  //    -> renvoient métriques + prédictions + modèle


  /** Régression linéaire */
  private def trainLinearRegression(
      train: DataFrame,
      test: DataFrame,
      evalRmse: RegressionEvaluator,
      evalR2: RegressionEvaluator
  ): (ModelMetrics, DataFrame, LinearRegressionModel) = {

    println("Entranement du modele de regression linieaire ")

    val lr = new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setPredictionCol("prediction")

    val model = lr.fit(train)

    println("=== Modele lineaire entraine ===")
    println(s"Intercept : ${model.intercept}")
    println("Coefficients (dans l'ordre des features) :")
    println("  (temp, dew, press, wnd_spd, snow, rain, hour, month)")
    println(s"  ${model.coefficients}")
    println()

    val predictions = model.transform(test)

    val rmse = evalRmse.evaluate(predictions)
    val r2   = evalR2.evaluate(predictions)

    println("=== Evaluation modele Regression lineaire ===")
    println(f"RMSE = $rmse%.3f")
    println(f"R²   = $r2%.3f")
    println("=== Exemple de predictions (lineaire) ===")
    predictions.select("label", "prediction").show(10, truncate = false)
    println()

    (ModelMetrics("Regression lineaire", rmse, r2), predictions, model)
  }

  /** Random Forest Regressor */
  private def trainRandomForest(
      train: DataFrame,
      test: DataFrame,
      evalRmse: RegressionEvaluator,
      evalR2: RegressionEvaluator
  ): (ModelMetrics, DataFrame, RandomForestRegressionModel) = {

    println("=== Entrainement du Random Forest Regressor ===")

    val rf = new RandomForestRegressor()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setNumTrees(60)
      .setMaxDepth(8)

    val model = rf.fit(train)

    println("Modele Random Forest entraine")
    println(s"Nombre d'arbres : ${model.getNumTrees}")
    println(s"Profondeur max  : ${model.getMaxDepth}")
    println()

    val predictions = model.transform(test)

    val rmse = evalRmse.evaluate(predictions)
    val r2   = evalR2.evaluate(predictions)

    println("=== Evaluation modele Random Forest ===")
    println(f"RMSE = $rmse%.3f")
    println(f"R²   = $r2%.3f")
    println("=== Exemple de predictions (Random Forest) ===")
    predictions.select("label", "prediction").show(10, truncate = false)
    println()

    (ModelMetrics("Random Forest", rmse, r2), predictions, model)
  }

  /** Gradient Boosted Trees Regressor */
  private def trainGBT(
      train: DataFrame,
      test: DataFrame,
      evalRmse: RegressionEvaluator,
      evalR2: RegressionEvaluator
  ): (ModelMetrics, DataFrame, GBTRegressionModel) = {

    println(" Entrainement du GBT Regressor (Gradient Boosted Trees) ")

    val gbt = new GBTRegressor()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMaxIter(60)
      .setMaxDepth(5)

    val model = gbt.fit(train)

    println("Modele GBT entraine ")
    println(s"Nombre d'iterations (arbres) : ${gbt.getMaxIter}")
    println(s"Profondeur max               : ${gbt.getMaxDepth}")
    println()

    val predictions = model.transform(test)

    val rmse = evalRmse.evaluate(predictions)
    val r2   = evalR2.evaluate(predictions)

    println("=== Evaluation modele GBT ===")
    println(f"RMSE = $rmse%.3f")
    println(f"R²   = $r2%.3f")
    println("=== Exemple de predictions (GBT) ===")
    predictions.select("label", "prediction").show(10, truncate = false)
    println()

    (ModelMetrics("Gradient Boosted Trees", rmse, r2), predictions, model)
  }

 
  // 3) Fonctions "un modele à la fois" (pour explication progressive)


  def runLinearOnly(df: DataFrame): ModelMetrics = {
    println("[Etape 1] Modele de regression lineaire")
    val data = prepareFeatures(df).select("features", "label")
    val Array(train, test) = data.randomSplit(Array(0.8, 0.2), seed = 42L)
    val (evalRmse, evalR2) = buildEvaluators()
    val (metrics, _, _)    = trainLinearRegression(train, test, evalRmse, evalR2)
    metrics
  }

  def runRandomForestOnly(df: DataFrame): ModelMetrics = {
    println("=== [Etape 2] Modele Random Forest ===")
    val data = prepareFeatures(df).select("features", "label")
    val Array(train, test) = data.randomSplit(Array(0.8, 0.2), seed = 42L)
    val (evalRmse, evalR2) = buildEvaluators()
    val (metrics, _, _)    = trainRandomForest(train, test, evalRmse, evalR2)
    metrics
  }

  def runGBTOnly(df: DataFrame): ModelMetrics = {
    println("=== [Etape 3] Modele Gradient Boosted Trees ===")
    val data = prepareFeatures(df).select("features", "label")
    val Array(train, test) = data.randomSplit(Array(0.8, 0.2), seed = 42L)
    val (evalRmse, evalR2) = buildEvaluators()
    val (metrics, _, _)    = trainGBT(train, test, evalRmse, evalR2)
    metrics
  }

  // 4) Fonctions de tracé avec XChart


  /** Courbe pollution réelle vs prédite (GBTR) */
  private def plotRealVsPred(predictions: DataFrame, title: String): Unit = {
    val rows = predictions
      .select("label", "prediction")
      .limit(200) // pour éviter un graphique illisible
      .collect()

    val idx = (1 to rows.length).map(Int.box).toList.asJava
    val real = rows.map(_.getDouble(0)).map(Double.box).toList.asJava
    val pred = rows.map(_.getDouble(1)).map(Double.box).toList.asJava

    val chart = new XYChartBuilder()
      .width(900)
      .height(600)
      .title(title)
      .xAxisTitle("Echantillon")
      .yAxisTitle("Pollution")
      .build()

    chart.addSeries("RReelle", idx, real)
    chart.addSeries("Predite", idx, pred)

    new SwingWrapper(chart).displayChart()
  }

  /** Courbe des résidus (erreurs = label - prediction) */
  private def plotResiduals(predictions: DataFrame, title: String): Unit = {
    val rows = predictions
      .select((col("label") - col("prediction")).alias("residual"))
      .limit(500)
      .collect()

    val idx = (1 to rows.length).map(Int.box).toList.asJava
    val resid = rows.map(_.getDouble(0)).map(Double.box).toList.asJava

    val chart = new XYChartBuilder()
      .width(900)
      .height(600)
      .title(title)
      .xAxisTitle("Echantillon")
      .yAxisTitle("Résidu (reelle - predite)")
      .build()

    chart.addSeries("RResidus", idx, resid)
    new SwingWrapper(chart).displayChart()
  }

  /** Barplot des RMSE des différents modèles */
  private def plotRmseBar(metrics: Seq[ModelMetrics]): Unit = {
    val names = metrics.map(_.name).toList.asJava
    val rmses = metrics.map(m => Double.box(m.rmse)).toList.asJava

    val chart = new CategoryChartBuilder()
      .width(800)
      .height(600)
      .title("Comparaison des modeles - RMSE")
      .xAxisTitle("Modele")
      .yAxisTitle("RMSE (plus bas = mieux)")
      .build()

    chart.addSeries("RMSE", names, rmses)

    new SwingWrapper(chart).displayChart()
  }

  /** Barplot des R² des différents modèles */
  private def plotR2Bar(metrics: Seq[ModelMetrics]): Unit = {
    val names = metrics.map(_.name).toList.asJava
    val r2s   = metrics.map(m => Double.box(m.r2)).toList.asJava

    val chart = new CategoryChartBuilder()
      .width(800)
      .height(600)
      .title("Comparaison des modeles - R²")
      .xAxisTitle("Modele")
      .yAxisTitle("R² (plus haut = mieux)")
      .build()

    chart.addSeries("R²", names, r2s)

    new SwingWrapper(chart).displayChart()
  }

  /** Feature importance pour un modèle d’arbres (RandomForest ou GBT) */
  private def plotFeatureImportances(
      modelName: String,
      importances: Vector
  ): Unit = {

    val values = importances.toArray
    val names  = featureCols.toList

    val chart = new CategoryChartBuilder()
      .width(900)
      .height(600)
      .title(s"Importances des variables - $modelName")
      .xAxisTitle("Feature")
      .yAxisTitle("Importance")
      .build()

    chart.addSeries(
      "Importance",
      names.asJava,
      values.map(Double.box).toList.asJava
    )

    new SwingWrapper(chart).displayChart()
  }


  // 5) Fonction globale : même split pour tous + comparaison + graphes


  /**
    * Utilise un seul split train/test pour les 3 modèles,
    * compare les performances, ET génère les graphes finaux.
    */
  def runAllModels(df: DataFrame): Unit = {
    println("=== Preparation des donnees pour les modeles ML ===")
    val data = prepareFeatures(df).select("features", "label")

    val Array(train, test) = data.randomSplit(Array(0.8, 0.2), seed = 42L)

    println(s"Nombre d'exemples train : ${train.count()}")
    println(s"Nombre d'exemples test  : ${test.count()}")
    println()

    val (evalRmse, evalR2) = buildEvaluators()

    // Entraînement sur le même split pour comparer vraiment
    val (mLinear, predsLinear, modelLinear) = trainLinearRegression(train, test, evalRmse, evalR2)
    val (mRF,     predsRF,     modelRF)     = trainRandomForest(train, test, evalRmse, evalR2)
    val (mGBT,    predsGBT,    modelGBT)    = trainGBT(train, test, evalRmse, evalR2)

    val allMetrics = Seq(mLinear, mRF, mGBT)

    println(" Comparaison des modeles (tries par RMSE croissant) :")
    println("Modele                        |   RMSE   |    R²")
    println("------------------------------+----------+--------")
    allMetrics.sortBy(_.rmse).foreach { m =>
      println(f"${m.name}%-28s | ${m.rmse}%8.3f | ${m.r2}%6.3f")
    }
    println()

    // ====== Graphes finaux ======

    // 1) Courbe pollution réelle vs prédite (modèle GBT)
    plotRealVsPred(predsGBT, "Pollution reelle vs predite (GBT)")

    // 2) Courbe des résidus pour le GBT
    plotResiduals(predsGBT, "RResidus du modele GBT")

    // 3) Barplots RMSE et R² pour tous les modèles
    plotRmseBar(allMetrics)
    plotR2Bar(allMetrics)

     // 4) Importances des variables pour RF et GBT
    plotFeatureImportances("Random Forest", modelRF.featureImportances)
    plotFeatureImportances("Gradient Boosted Trees", modelGBT.featureImportances)

    // Pause pour éviter que le programme se termine avant que tu voies les graphes
    println("Graphiques ML affiches. Appuie sur Entree pour terminer le programme…")
    StdIn.readLine()

  }
}
