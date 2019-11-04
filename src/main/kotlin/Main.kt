package ru.chenko

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.*
import scala.collection.Seq

fun main(args:Array<String>) {
    if(args.isEmpty()) {
        println("Usage: java -jar jarName filePath")
        return
    }
    val datasetPath = args[0]

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
            DataTypes.createStructField("time", DataTypes.DateType, false),
            DataTypes.createStructField("value", DataTypes.FloatType, false)))

    val dataset = spark.read()
            .format("csv")
            .option("header", "true")
            .option("dateFormat", "dd.MM.yy HH:mm")
            .schema(datasetSchema)
            .csv(datasetPath)
    dataset.show(20)
    dataset.printSchema()


}