package ru.chenko

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType

fun main(args:Array<String>) {
    if(args.size < 2) {
        println("Usage: java -jar jarName filePath byMinutesAggregate")
        return
    }
    val datasetPath = args[0]
    val minutes = args[1].toInt();

    val conf:SparkConf = SparkConf()
            .setMaster("local")
            .setAppName("Kotlin Spark")

    val sparkContext:JavaSparkContext = JavaSparkContext(conf)

    val spark:SparkSession = SparkSession
            .builder()
            .appName("Kotlin Spark")
            .orCreate

    val datasetSchema:StructType = DataTypes.createStructType(listOf(
            DataTypes.createStructField("id", DataTypes.IntegerType, false),
            DataTypes.createStructField("time", DataTypes.TimestampType, false),
            DataTypes.createStructField("value", DataTypes.FloatType, false)))

    val dataset = spark.read()
            .format("csv")
            .option("header", "true")
            .schema(datasetSchema)
            .csv(datasetPath)

    dataset.printSchema()


    val windowDataset = dataset
            .groupBy(col("id"), window(col("time"),"$minutes minutes"))
            .agg(avg("value").`as`("avg_value"))
            .withColumn("time", col("window.start"))
            .drop("window")
            .sort(col("id"), col("time"))
            .select(col("id"), col("time"), col("avg_value"))


    windowDataset.printSchema()
    windowDataset
            .coalesce(1)
            .write()
            .format("csv")
            .mode(SaveMode.Append)
            .option("header", true)
            .csv("result")
}
