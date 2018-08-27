import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import org.apache.spark.sql.functions._

object Part3 {

  //var rdd :RDD[String]= null

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: Part1 InputDir OutputDir")
    }

    val sc = new SparkContext(new SparkConf().setAppName("Part3"))
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val sqlContext = spark.sqlContext
    import spark.implicits._
    val prodorders = spark.read.option("header", "true").csv(args(0))
    val prods = spark.read.option("header", "true").csv(args(1))
    val prod_order = prodorders.select("order_id", "product_id")

    val joinData = prod_order.join(prods , prodorders.col("product_id") === prods.col("product_id"), "left").sort($"order_id").toDF().select(prodorders.col("order_id"), prods.col("product_name"))

    //joinData.show()
    val rows = joinData.groupBy($"order_id").agg(collect_set("product_name").as("list_of_prods")).select("list_of_prods")
    rows.count()
    val training = rows.filter($"list_of_prods".isNotNull)
    // // training.show()
    // val dataset = spark.createDataset(Seq(
    //   "1 2 5",
    //   "1 2 3 5",
    //   "1 2")
    // ).map(t => t.split(" ")).toDF("items")
    // // dataset.show()

    val fpgrowth = new FPGrowth().setItemsCol("list_of_prods").setMinSupport(0.0007697642).setNumPartitions(10)
    val model = fpgrowth.fit(rows)

    // // Display frequent itemsets.
    var output = model.freqItemsets.sort(desc("freq")).take(10)

    val output_rdd=sc.parallelize(output)
    val srcop = output_rdd.map(_.mkString(","))
    //val op= output.rdd.map(_.toString().replace("[","").replace("]", "")).coalesce(1,true).saveAsTextFile(args(2))

    val fpgrowthConf = new FPGrowth().setItemsCol("list_of_prods").setMinSupport(0.0007697642).setMinConfidence(0.2)
    val modelConf = fpgrowthConf.fit(rows)

    val tempassoc = modelConf.associationRules.sort(desc("confidence")).take(10)
    val opassoc= sc.parallelize(tempassoc)

    var out="\n" +"The Association rules items are\t" + "\n"
    val out1=sc.parallelize(List(out))
    val src = opassoc.map(_.mkString(","))

    var output1 = srcop ++ out1 ++ src
    //val opassoc= tempassoc.rdd.map(_.toString().replace("[","").replace("]", "")).coalesce(1,true).saveAsTextFile(args(2))

    output1.coalesce(1,true).saveAsTextFile(args(2))
  }
}