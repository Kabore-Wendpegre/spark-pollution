
#  Analyse et Prédiction de la Pollution Urbaine avec Spark

##  Présentation

Ce projet a pour objectif d’**analyser et de prédire la pollution atmosphérique** à partir de données environnementales massives.
Il illustre l’utilisation de la **programmation fonctionnelle** avec **Apache Spark** pour le traitement Big Data, l’analyse exploratoire, la modélisation par graphes et la mise en œuvre de modèles de **Machine Learning**.


## Objectifs du projet

* Ingestion et nettoyage de données de pollution
* Analyse exploratoire des données (EDA)
* Étude des variations temporelles de la pollution
* Modélisation de la propagation de la pollution avec **GraphX**
* Prédiction de la pollution via **Spark MLlib**
* Comparaison de plusieurs modèles de Machine Learning



## Données utilisées

Le projet s’appuie sur un jeu de données de pollution multivariée contenant des **mesures horaires**, incluant notamment :

* Pollution (variable cible)
* Température
* Point de rosée (*dew*)
* Pression atmosphérique
* Vitesse et direction du vent
* Indicateurs de pluie et de neige
* Variables temporelles (heure, jour, mois, jour de la semaine)

---

## Technologies utilisées

* **Scala 2.12**
* **Apache Spark 3.5.1**

  * Spark SQL
  * Spark MLlib
  * Spark GraphX
* **XChart** pour les visualisations
* **GraphViz** pour le rendu des graphes
* **sbt** pour la gestion du projet

---

## ▶️ Exécution du projet

### Prérequis

* Java **11**
* sbt **1.8+**
* GraphViz installé (`dot` accessible en ligne de commande)
*hadoop

### Lancer le projet

```bash
sbt run
```

Le programme exécute automatiquement :

1. Le chargement et le nettoyage des données
2. L’analyse exploratoire (EDA)
3. La modélisation par graphe (GraphX)
4. L’entraînement et l’évaluation des modèles ML
5. La génération des visualisations

---

## Analyses réalisées

### 🔍 Analyse exploratoire

* Pollution moyenne par heure, jour et mois
* Corrélations entre pollution et variables météorologiques
* Identification des périodes critiques
* Répartition des niveaux de pollution (Low / Medium / High)

###  Modélisation par graphe (GraphX)

* Définition de stations temporelles (Nuit, Matin, Journée, Après-midi, Soir)
* Construction d’un graphe de propagation
* Simulation de la diffusion de la pollution
* Visualisation des graphes

###  Machine Learning

* Régression linéaire
* Random Forest Regressor
* Gradient Boosted Trees Regressor
* Comparaison des performances (RMSE, R²)
* Analyse de l’importance des variables



## Résultats principaux

* Le modèle **Gradient Boosted Trees** obtient les meilleures performances
* La pollution est fortement influencée par la **saisonnalité (month)** et le **point de rosée (dew)**
* Les modèles non linéaires surpassent la régression linéaire



##  Limites et perspectives

* Le traitement en temps réel (streaming) n’a pas été implémenté (extension optionnelle)
* La direction du vent pourrait être mieux exploitée via un encodage catégoriel
* L’ajout de variables retardées (*lags temporels*) pourrait améliorer les prédictions
