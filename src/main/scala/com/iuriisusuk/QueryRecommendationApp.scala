package com.iuriisusuk

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.fpm.FPGrowth

import org.apache.spark.sql.functions._

object QueryRecommendationApp {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("Ranking Application")
      .getOrCreate

    import spark.implicits._

    case class Init(id: String, query: String)
    case class Init1(id: String, queries: Seq[String])

    val dataset = spark.read.format("csv")
      .load("file:////Users/ysusuk/vidiq/wikimedia/src/main/resources/queries.csv")
      .toDF("id", "queries").rdd


    val dataset1 = dataset
      .map(r => Init(r.getString(0), r.getString(1)))
      .groupBy(_.id)
      .mapValues(inits => inits.map(_.query).toSet)
      .filter(t => t._2.size > 1)
      .toDF("id", "queries")
    //.map(t => Init1(t._1, t._2.map(_.queries)]))

    //    val dataset1 = dataset.withColumn("queries", split(dataset("queries"), " ").cast("array<string>"))

    dataset1.show()

    val fpgrowth = new FPGrowth().setItemsCol("queries").setMinSupport(0.1).setMinConfidence(0.1)
    val model = fpgrowth.fit(dataset1)

    // Display frequent itemsets.
    model.freqItemsets.show()

    // Display generated association rules.
    // model.associationRules.show()

    // transform examines the input items against all the association rules and summarize the
    // consequents as prediction
    model.transform(dataset1).show()

    spark.stop
  }
}
