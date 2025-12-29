\# Projet Spark – Analyse et Prévision de la Pollution



\## Objectif

Développer une application Apache Spark (Scala) capable de :

\- Charger des données réelles de pollution (dataset Kaggle).

\- Nettoyer et préparer les données.

\- Produire des statistiques (pollution par heure/jour).

\- Détecter des anomalies.

\- Construire un modèle MLlib pour prédire la pollution (PM2.5).



\## Structure du projet



spark-pollution/

├── build.sbt

├── project/

│   └── plugins.sbt

├── src/main/scala/com/example/pollution/

│   └── Main.scala      (point d'entrée du projet, pour l’instant)

├── data/input/         (données brutes, ex : pollution\_raw.csv)

├── data/processed/     (données nettoyées, format Parquet)

├── outputs/            (résultats des analyses)

└── models/             (modèles de ML sauvegardés)



\## Pré-requis

\- Java (JDK 17)

\- sbt

\- Apache Spark 3.5.1



