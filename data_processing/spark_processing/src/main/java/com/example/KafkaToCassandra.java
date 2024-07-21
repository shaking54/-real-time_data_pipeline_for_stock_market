package com.example;
import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;
import org.apache.spark.sql.types.DataTypes;

public class KafkaToCassandra {

    public static void main(String[] args) throws StreamingQueryException, TimeoutException, InterruptedException {
        // Initialize Spark session
        SparkSession spark = SparkSession.builder()
                .appName("SparkKafkaToCassandraStructuredStreaming")
                .master("local[*]")
                .config("spark.cassandra.connection.host", "127.0.0.1")
                .getOrCreate();

        // Define the schema for the JSON data
        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("TimeStamp", DataTypes.FloatType)
                .add("Open", DataTypes.FloatType)
                .add("High", DataTypes.FloatType)
                .add("Low", DataTypes.FloatType)
                .add("Close", DataTypes.FloatType)
                .add("Volume", DataTypes.FloatType);
        ;
        // Read data from Kafka
        Dataset<Row> df = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "stock_data_2")
                .option("failOnDataLoss", "false")
                .option("startingOffsets", "latest")
                .load()
                .selectExpr("CAST(value AS STRING)")
                .select(functions.from_json(functions.col("value"), schema).as("data"))
                .select("data.*");
        
        
        //Write data to Cassandra
        StreamingQuery query = df.filter(col("id").isNotNull()).writeStream()
                .outputMode("append")
                .format("org.apache.spark.sql.cassandra")
                .option("checkpointLocation", "/tmp/shaking/checkpoint")
                .option("keyspace", "stock_data")
                .option("table", "prices")
                .start();
        query.awaitTermination();
    }
}
        
