package ru.chenko

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.api.java.JavaStreamingContext
import scala.Console.println

fun main(args:Array<String>) {
    if(args.isEmpty()) {
        println("Usage: java -jar jarName byMinutesAggregate")
        return
    }

    val minutes = args[0].toInt()

    val conf:SparkConf = SparkConf()
            .setMaster("local")
            .setAppName("Kotlin Spark")

    var streamingContext:JavaStreamingContext = JavaStreamingContext(conf, org.apache.spark.streaming.Duration(60*1000));

    val spark:SparkSession = SparkSession
            .builder()
            .appName("Kotlin Spark")
            .orCreate

    //Схема входных данных (метрик)
    val datasetSchema:StructType = DataTypes.createStructType(listOf(
            DataTypes.createStructField("id", DataTypes.IntegerType, false),
            DataTypes.createStructField("time", DataTypes.TimestampType, false),
            DataTypes.createStructField("value", DataTypes.FloatType, false)))

    //Считываем данные из топика Kafka в датасет в режиме стрима
    val dataset = spark.readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "spark")
            .option("startingOffsets", "earliest")
            .load()

    //Вычленияем из считанных из Kafka данных интересующие нас данные (избавляемся от вложенности полей)
    val metricDataset= dataset
            .select(from_json(col("value").cast("string"), datasetSchema))
            .withColumn("metric", col("jsontostructs(CAST(value AS STRING))"))
            .drop("jsontostructs(CAST(value AS STRING))")
            .select(col("metric.id"), col("metric.time"), col("metric.value"))

    //Агрегируем данные по столбцу time и преобразуем результат к исходной структуре
    val windowDataset = metricDataset
            .withWatermark("time", "$minutes minutes")
            .groupBy(col("id"), window(col("time"),"${minutes*2} minutes", "$minutes minutes"))
            .agg(avg("value").`as`("avg_value"))
            .withColumn("time", col("window.start"))
            .drop("window")
            .select(col("id"), col("time"), col("avg_value"))

    //Записываем результат в csv файл
    windowDataset
            .coalesce(1)
            .writeStream()
            .format("csv")
            .option("checkpointLocation", "result/checkpoint/")
            .option("path", "result/output/")
            .option("format", "append")
            .outputMode("append")
            .start()
            .awaitTermination()
}
