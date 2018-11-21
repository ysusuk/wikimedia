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

    val dataset = spark.read.format("csv")
      .load("file:////Users/ysusuk/vidiq/wikimedia/pois.csv")
      .toDF("name", "lat", "long", "content")

    val dataset1 = dataset.withColumn("name", split(dataset("name"), " ").cast("array<string>"))
    // val dataset2 = dataset1.withColumn("name", collect_set(dataset1("name")))

    dataset1.show()

    val fpgrowth = new FPGrowth().setItemsCol("name").setMinSupport(0.5).setMinConfidence(0.5)
    val model = fpgrowth.fit(dataset1)

    // Display frequent itemsets.
    // model.freqItemsets.show()

    // Display generated association rules.
    // model.associationRules.show()

    // transform examines the input items against all the association rules and summarize the
    // consequents as prediction
    model.transform(dataset1).show()

    spark.stop
  }
}
