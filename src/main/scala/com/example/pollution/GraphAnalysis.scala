package com.example.pollution

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io.{File, PrintWriter}
import java.awt.Desktop

/**
  * Analyse de la pollution sous forme de graphe avec GraphX.
  *
  * Idée générale :
  *   - On regroupe les heures de la journée en 5 "stations" (Nuit, Matin, etc.).
  *   - On calcule la pollution moyenne pour chaque station.
  *   - On construit un graphe dont les sommets sont ces stations.
  *   - On simule une "propagation" de la pollution entre stations voisines.
  *   - On exporte ce graphe au format GraphViz (.dot) et on génère des images .png.
  */
object GraphAnalysis {


  // 1) CONSTRUCTION DU GRAPHE À PARTIR DU DATAFRAME


  /**
    * Construit un graphe GraphX à partir du DataFrame final.
    *
    * @param df DataFrame contenant au minimum les colonnes :
    *           - "hour" : heure de la mesure
    *           - "pollution" : niveau de pollution
    *
    * Étapes :
    *   1. On assigne chaque ligne à une station en fonction de l'heure (1 à 5).
    *   2. On calcule la pollution moyenne par station.
    *   3. On crée les sommets (vertices) : (idStation, (nomStation, pollutionMoyenne)).
    *   4. On crée des connexions artificielles (arêtes) entre les stations.
    *   5. On retourne le graphe GraphX.
    */
  def buildStationsGraph(df: DataFrame): Graph[(String, Double), Double] = {
    val spark = df.sparkSession
    import spark.implicits._

    // 1) Affectation d'un identifiant de station en fonction de l'heure
    val stationDf = df
      .withColumn(
        "station_id",
        when(col("hour") <= 4, lit(1L))   // 0–4h  -> station 1 : Nuit
          .when(col("hour") <= 8, lit(2L))  // 5–8h  -> station 2 : Matin
          .when(col("hour") <= 12, lit(3L)) // 9–12h -> station 3 : Milieu de journée
          .when(col("hour") <= 18, lit(4L)) // 13–18h -> station 4 : Après-midi
          .otherwise(lit(5L))               // 19–23h -> station 5 : Soir
      )
      // 2) Pollution moyenne par station
      .groupBy("station_id")
      .agg(avg("pollution").alias("avg_pollution"))
      // 3) Ajout d'un nom lisible pour chaque station
      .withColumn(
        "station_name",
        when(col("station_id") === 1L, lit("Nuit (0–4h)"))
          .when(col("station_id") === 2L, lit("Matin (5–8h)"))
          .when(col("station_id") === 3L, lit("Milieu de journee (9–12h)"))
          .when(col("station_id") === 4L, lit("Apres-midi (13–18h)"))
          .otherwise(lit("Soir (19–23h)"))
      )

    // 4) Sommets du graphe :
    //    Chaque sommet est identifié par un VertexId (alias de Long)
    //    et porte comme attribut (nomStation, pollutionMoyenne).
    val vertices: RDD[(VertexId, (String, Double))] =
      stationDf
        .select("station_id", "station_name", "avg_pollution")
        .as[(Long, String, Double)] // Dataset typé
        .rdd                        // Conversion en RDD pour GraphX
        .map { case (id, name, avgPoll) =>
          (id, (name, avgPoll))
        }

    // 5) Arêtes du graphe :
    //    On définit des connexions "logiques" entre les stations.
    //    Le poids de l'arête (Double) peut représenter la force du lien.
    val edges: RDD[Edge[Double]] = spark.sparkContext.parallelize(Seq(
      Edge(1L, 2L, 1.0),  // Nuit  -> Matin
      Edge(2L, 3L, 1.0),  // Matin -> Milieu de journée
      Edge(3L, 4L, 1.0),  // Milieu de journée -> Après-midi
      Edge(4L, 5L, 1.0),  // Après-midi -> Soir
      Edge(1L, 3L, 0.5),  // Nuit  -> Milieu de journée (lien plus faible)
      Edge(3L, 5L, 0.5)   // Milieu de journée -> Soir
    ))

    // 6) Construction finale du graphe GraphX
    Graph(vertices, edges)
  }

  // 2) AFFICHAGE TEXTE DU GRAPHE


  /**
    * Affiche dans la console :
    *   - la liste des stations (sommets) avec leur pollution moyenne,
    *   - la liste des connexions (arêtes) avec leurs poids.
    *
    * Utile pour vérifier rapidement la structure du graphe.
    */
  def showGraphInfo(title: String, graph: Graph[(String, Double), Double]): Unit = {
    println(s"$title ")

    println("Stations :")
    graph.vertices.collect().foreach { case (id, (name, poll)) =>
      // f"" permet de formater les nombres (2 décimales, etc.)
      println(f"  $id%2d : $name%-30s pollution moyenne = $poll%6.2f")
    }

    println("Connexions :")
    graph.edges.collect().foreach { e =>
      println(s"  ${e.srcId} -> ${e.dstId} (poids = ${e.attr})")
    }

    println()
  }

  // 3) PROPAGATION DE LA POLLUTION DANS LE GRAPHE


  /**
    * Applique une étape de "propagation" de la pollution dans le graphe.
    *
    * Idée :
    *   - chaque station regarde la pollution de ses voisines,
    *   - calcule la moyenne de ses voisines,
    *   - puis met à jour sa valeur comme moyenne entre :
    *       (sa propre pollution, la moyenne de ses voisines).
    *
    * @param graph graphe initial
    * @return nouveau graphe avec pollution mise à jour.
    */
  def propagatePollution(graph: Graph[(String, Double), Double]):
    Graph[(String, Double), Double] = {

    // 1) Pour chaque sommet, on veut :
    //    (somme des pollutions des voisins, nombre de voisins)
    val neighAgg: VertexRDD[(Double, Int)] =
      graph.aggregateMessages[(Double, Int)](
        triplet => {
          // Chaque arête (u -> v) envoie :
          //  - la pollution de u vers v
          //  - la pollution de v vers u
          triplet.sendToDst((triplet.srcAttr._2, 1))
          triplet.sendToSrc((triplet.dstAttr._2, 1))
        },
        // Fonction de réduction : on additionne les pollutions et les compteurs
        (a, b) => (a._1 + b._1, a._2 + b._2)
      )

    // 2) Jointure entre :
    //    - les sommets (nom, pollution propre)
    //    - les agrégats des voisins (somme, nb)
    val newVertices = graph.vertices.leftOuterJoin(neighAgg).mapValues {
      case ((name, ownPoll), Some((sumNeigh, countNeigh))) =>
        val avgNeigh = sumNeigh / countNeigh           // moyenne des voisins
        val newPoll  = (ownPoll + avgNeigh) / 2.0      // moyenne entre soi et voisins
        (name, newPoll)

      case ((name, ownPoll), None) =>
        // Si un sommet n'a pas de voisins (cas théorique ici),
        // on garde sa pollution inchangée.
        (name, ownPoll)
    }

    // 3) On retourne un nouveau graphe avec :
    //    - mêmes arêtes,
    //    - sommets mis à jour.
    Graph(newVertices, graph.edges)
  }

  // 4) EXPORT EN FORMAT GRAPHVIZ (.dot)


  /**
    * Exporte le graphe au format GraphViz (.dot).
    *
    * @param graph graphe GraphX à exporter
    * @param path  chemin du fichier .dot (ex : "output/stations_initial.dot")
    * @param title titre affiché en haut du graphe dans GraphViz
    *
    * Le fichier .dot pourra ensuite être converti en image (PNG, PDF, etc.)
    * avec l'outil `dot` de GraphViz.
    */
  def exportGraphToDot(
      graph: Graph[(String, Double), Double],
      path: String,
      title: String
  ): Unit = {

    val file = new File(path)
    val parentDir = file.getParentFile
    if (parentDir != null && !parentDir.exists()) parentDir.mkdirs()

    val pw = new PrintWriter(file)

    // En-tête du graphe DOT
    pw.println("digraph Stations {")
    pw.println(s"""  label = "$title";""")
    pw.println("""  labelloc = "t";""")
    pw.println("""  fontsize = 18;""")
    pw.println()
    pw.println("""  node [shape=circle, style=filled, fillcolor="#87CEEB", fontname="Helvetica"];""")
    pw.println("""  edge [fontname="Helvetica"];""")
    pw.println()

    // Noeuds : id, nom, pollution moyenne
    graph.vertices.collect().foreach { case (id, (name, poll)) =>
      val label = s"$id\\n$name\\n${"%.2f".format(poll)}"
      pw.println(s"""  $id [label="$label"];""")
    }

    pw.println()

    // Arêtes : src -> dst [label=poids]
    graph.edges.collect().foreach { e =>
      pw.println(s"""  ${e.srcId} -> ${e.dstId} [label="${e.attr}"];""")
    }

    pw.println("}")
    pw.close()

    println(s"Fichier DOT exporte dans : $path")
  }

  // 5) APPEL AUTOMATIQUE À GRAPHVIZ (dot) + OUVERTURE DU PNG


  /**
    * Essaie d'exécuter l'outil `dot` de GraphViz avec plusieurs chemins possibles.
    *
    * @param args arguments à passer à `dot` (ex : "-Tpng", input, "-o", output)
    * @return true si l'exécution s'est bien passée (code de retour = 0)
    *
    * But : permettre un lancement automatique même si GraphViz n'est pas
    * forcément dans le PATH mais installé dans Program Files.
    */
  private def runDot(args: Seq[String]): Boolean = {
    val candidates = Seq(
      "dot",                                              // si GraphViz est dans le PATH
      "C:\\Program Files\\Graphviz\\bin\\dot.exe",        // installation classique 64 bits
      "C:\\Program Files (x86)\\Graphviz\\bin\\dot.exe"   // autre cas fréquent
    )

    for (exe <- candidates) {
      try {
        val cmd = exe +: args
        val pb  = new ProcessBuilder(cmd: _*).redirectErrorStream(true)
        val p   = pb.start()
        val exit = p.waitFor()
        if (exit == 0) {
          println(s"[OK] GraphViz execute avec : $exe")
          return true
        } else {
          println(s"[WARN] Echec de GraphViz avec $exe (code = $exit)")
        }
      } catch {
        case e: Exception =>
          println(s"[WARN] Impossible de lancer $exe : ${e.getMessage}")
      }
    }

    println("[ERREUR] Impossible d'executer GraphViz (dot). Verifie son installation.")
    false
  }

  /**
    * Génère une image PNG à partir d'un fichier .dot,
    * puis ouvre automatiquement l'image avec la visionneuse par défaut.
    *
    * @param dot chemin du fichier .dot en entrée
    * @param png chemin du fichier .png de sortie
    */
  def generatePngAndOpen(dot: String, png: String): Unit = {
    val ok = runDot(Seq("-Tpng", dot, "-o", png))

    if (ok) {
      val f = new File(png).getAbsoluteFile
      println(s"[OK] PNG genere : $f")

      try {
        if (Desktop.isDesktopSupported) {
          Desktop.getDesktop.open(f)
          println(s"[OK] Image ouverte automatiquement.")
        } else {
          println("[INFO] Desktop non supporte, ouvre l'image manuellement.")
        }
      } catch {
        case e: Exception =>
          println(s"[WARN] Impossible d'ouvrir automatiquement l'image : ${e.getMessage}")
      }
    }
  }


  /**
    * Fonction appelée depuis Main.scala.
    *
    * Enchaîne :
    *   1. Construction du graphe des stations.
    *   2. Affichage des infos du graphe initial.
    *   3. Export en DOT + génération + ouverture de l'image PNG.
    *   4. Propagation d'une étape de pollution.
    *   5. Affichage du graphe propagé.
    *   6. Export en DOT + génération + ouverture de l'image propagée.
    */
  def runGraph(df: DataFrame): Unit = {
    // Graphe initial
    val graph = buildStationsGraph(df)
    showGraphInfo("Graphe initial des stations", graph)

    val initialDot = "output/stations_initial.dot"
    val initialPng = "output/stations_initial.png"
    exportGraphToDot(graph, initialDot, "Stations - Graphe initial")
    generatePngAndOpen(initialDot, initialPng)

    // Graphe après propagation
    val propagated = propagatePollution(graph)
    showGraphInfo("Graphe apres propagation", propagated)

    val propagatedDot = "output/stations_propagated.dot"
    val propagatedPng = "output/stations_propagated.png"
    exportGraphToDot(propagated, propagatedDot, "Stations - Apres propagation")
    generatePngAndOpen(propagatedDot, propagatedPng)
  }
}
