package com.cabin.pizza

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Analysis {

  val pizzasFile = "data/pizzas.json"

  val ordersFile = "data/orders.json"

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .master("local")
      .appName("Pizza Cabin")
      .getOrCreate()

    bbqPizzas(spark)

    println("-------------------------------------")
  }

  def bbqPizzas(spark: SparkSession): Unit = {

    val pizzas = spark.read.json(pizzasFile)

    pizzas
      .filter(pizzas("sauce") === "bbq")
      .show()
  }

  // use spark implicits to reference the DataFrame with $
  def tomatoPizzas(spark: SparkSession): Unit = {
    import spark.implicits._

    spark.read.json(pizzasFile)
      .filter($"sauce" === "tomato")
      .show()
  }

  // combine filters
  def vegetarianTomatoPizzas(spark: SparkSession): Unit = {
    import spark.implicits._

    spark.read.json(pizzasFile)
      .filter($"sauce" === "tomato" && $"vegetarian")
      .show()
  }


  def mostPopularPizzas(spark: SparkSession): Unit = {
    import spark.implicits._

    spark.read.json(ordersFile)
      .groupBy($"pizzaId")
      .count
      .sort($"count")
      .show
  }

  def mostPopularPizzasWithName(spark: SparkSession): Unit = {
    import spark.implicits._

    val pizzas = spark.read.json(pizzasFile)

    spark.read.json(ordersFile)
      .join(pizzas, $"pizzaId" === pizzas("id"))
      .groupBy($"pizzaId")
      .agg(
        first("name") as "name",
        count("*") as "count"
      )
      .sort($"count".desc)
      .show
  }

  def highestRated(spark: SparkSession): Unit = {
    import spark.implicits._

    val pizzas = spark.read.json(pizzasFile)

    spark.read.json(ordersFile)
      .groupBy($"pizzaId")
      .agg(
        mean("rating") as "avgRating"
      )
      .join(pizzas, $"pizzaId" === pizzas("id"))
      .select("name", "avgRating")
      .sort($"avgRating".desc)
      .show()
  }

  // This is not representative of the toppings that are required - based on orders
  // This is left as an exercise for the reader!
  def whatToppingsToBuy(spark: SparkSession): Unit = {
    import spark.implicits._

    spark.read.json(pizzasFile)
      .withColumn("topping", explode($"toppings"))
      .groupBy($"topping")
      .count
      .show
  }

}