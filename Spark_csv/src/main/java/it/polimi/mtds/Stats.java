package it.polimi.mtds;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import it.polimi.mtds.utils.LogUtils;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * Input: csv files containing sensor readings
 * schema ("location: String, dateTime: Timestamp, temperature: Float, humidity: Float)
 */
public class Stats {
    public static void main(String[] args) {
        LogUtils.setLogLevel();

        final String master = args.length > 0 ? args[0] : "local[4]";
        final String filePath = args.length > 1 ? args[1] : "./";
        final String appName = "it.polimi.mtds.Stats";

        final SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName(appName)
                .getOrCreate();

        final List<StructField> mySchemaFields = new ArrayList<>();
        mySchemaFields.add(DataTypes.createStructField("location", DataTypes.StringType, true));
        mySchemaFields.add(DataTypes.createStructField("dateTime", DataTypes.TimestampType, true));
        mySchemaFields.add(DataTypes.createStructField("temperature", DataTypes.FloatType, true));
        mySchemaFields.add(DataTypes.createStructField("humidity", DataTypes.FloatType, true));
        final StructType mySchema = DataTypes.createStructType(mySchemaFields);

        Dataset<Row> dataset = spark
                .read()
                .option("header", "true")
                .option("delimiter", ";")
                .option("dateFormat","dd/MM/yyyy HH:mm:ss")
                .schema(mySchema)
                .csv(filePath + "../DataOut/dataset.csv");

        WindowSpec w = (Window.partitionBy(col("room")).orderBy(col("dateTime").cast(DataTypes.LongType)).rangeBetween(-7, 0));

        dataset = dataset.withColumn("neighborhood",split(col("location"),"[.]").getItem(0))
                .withColumn("building",split(col("location"),"[.]").getItem(1))
                .withColumn("floor",split(col("location"),"[.]").getItem(2))
                .withColumn("room",split(col("location"),"[.]").getItem(3))
                .withColumn("fullFloorLocation", concat_ws(".", col("neighborhood"), col("building"), col("floor")))
                .withColumn("fullBuildingLocation", concat_ws(".", col("neighborhood"), col("building")))
                .withColumnRenamed("location", "fullRoomLocation")
                .withColumn("hour", hour(col("dateTime")))
                .withColumn("day", to_date(col("dateTime")))
                .withColumn("week", weekofyear(col("dateTime")))
                .withColumn("month", month(col("dateTime")))
                .withColumn("year", year(col("dateTime")))
                .withColumn("night", hour(col("dateTime")).cast(DataTypes.IntegerType).lt(8).or(hour(col("dateTime")).cast(DataTypes.IntegerType).geq(20)))
                .withColumn("daily", hour(col("dateTime")).cast(DataTypes.IntegerType).lt(20).and(hour(col("dateTime")).cast(DataTypes.IntegerType).geq(8)))
        ;
        dataset.show();
        dataset.cache();

        //Hourly moving average - Room
        final Dataset<Row> movingAverageTemperatureAndHumidityHour = dataset
                .groupBy("hour", "day", "room")
                .agg(
                        avg("temperature").as("avg_temp"),
                        avg("humidity").as("avg_hum")
                )
                .orderBy("day", "hour");

        //movingAverageTemperatureAndHumidityHour.withColumn("rolling_average", avg("dollars").over(w));



        //Hourly moving average - Building
        final Dataset<Row> hourlyBuilding = dataset
                .groupBy("hour", "day", "building")
                .agg(
                        avg("temperature").as("avg_temp"),
                        avg("humidity").as("avg_hum")
                )
                .orderBy("day","hour");
        hourlyBuilding.show();

        //Hourly moving average - Building Level



        //Hourly moving average - Neighborhood-scale



        //Daily moving average - Room
        final Dataset<Row> movingAverageTemperatureAndHumidityDaily = dataset
                .groupBy("week")
                .agg(
                        avg("temperature").as("avg_temp"),
                        avg("humidity").as("avg_hum")
                )
                .orderBy("week");




        //Daily moving average - Building



        //Daily moving average - Building Level



        //Daily moving average - Neighborhood-scale



        //Weekly moving average - Room
        final Dataset<Row> movingAverageTemperatureAndHumidityWeekly = dataset
                .groupBy("week")
                .agg(
                        avg("temperature").as("avg_temp"),
                        avg("humidity").as("avg_hum")
                )
                .orderBy("week");



        //Weekly moving average - Building Level



        //Weekly moving average - Building



        //Weekly moving average - Neighborhood-scale



        //Hourly moving average - Neighborhood-scale



        //Daily night-day temperature difference - Room
        final Dataset<Row> meanDailyRoom = dataset
                .groupBy("daily", "day", "fullRoomLocation")
                .agg(
                        avg("temperature").as("avg_temp"),
                        avg("humidity").as("avg_hum")
                )
                .orderBy("day");

        final Dataset<Row> dailyRoom = meanDailyRoom.filter(col("daily").equalTo(true)).withColumnRenamed("avg_temp", "day_temp").withColumnRenamed("avg_hum", "day_hum");
        final Dataset<Row> nightRoom = meanDailyRoom.filter(col("daily").equalTo(false)).withColumnRenamed("fullRoomLocation", "fl").withColumnRenamed("day", "d").withColumnRenamed("avg_temp", "night_temp").withColumnRenamed("avg_hum", "night_hum");

        final Dataset<Row> diffRoom = dailyRoom.join(nightRoom, nightRoom.col("fl").equalTo(dailyRoom.col("fullRoomLocation")).and(nightRoom.col("d").equalTo(dailyRoom.col("day"))), "left_outer")
                .filter(col("d").isNotNull())
                .drop("daily", "d", "fl")
                .withColumn("temp_diff", expr("day_temp-night_temp"))
                .withColumn("hum_diff", expr("day_hum-night_hum"));
        diffRoom.cache();


        //Daily night-day temperature difference - Floor Level
        final Dataset<Row> meanDailyFloor = dataset
                .groupBy("daily", "day", "fullFloorLocation")
                .agg(
                        avg("temperature").as("avg_temp"),
                        avg("humidity").as("avg_hum")
                )
                .orderBy("day");

        final Dataset<Row> dailyFloor = meanDailyFloor.filter(col("daily").equalTo(true)).withColumnRenamed("avg_temp", "day_temp").withColumnRenamed("avg_hum", "day_hum");
        final Dataset<Row> nightFloor = meanDailyFloor.filter(col("daily").equalTo(false)).withColumnRenamed("fullFloorLocation", "fl").withColumnRenamed("day", "d").withColumnRenamed("avg_temp", "night_temp").withColumnRenamed("avg_hum", "night_hum");

        final Dataset<Row> diffFloor = dailyFloor.join(nightFloor, nightFloor.col("fl").equalTo(dailyFloor.col("fullFloorLocation")).and(nightFloor.col("d").equalTo(dailyFloor.col("day"))), "left_outer")
                .filter(col("d").isNotNull())
                .drop("daily", "d", "fl")
                .withColumn("temp_diff", expr("day_temp-night_temp"))
                .withColumn("hum_diff", expr("day_hum-night_hum"));
        diffFloor.cache();
        
        //Daily night-day temperature difference - Building Level
        final Dataset<Row> meanDailyBuilding = dataset
                .groupBy("daily", "day", "fullBuildingLocation")
                .agg(
                        avg("temperature").as("avg_temp"),
                        avg("humidity").as("avg_hum")
                )
                .orderBy("day");

        final Dataset<Row> dailyBuilding = meanDailyBuilding.filter(col("daily").equalTo(true)).withColumnRenamed("avg_temp", "day_temp").withColumnRenamed("avg_hum", "day_hum");
        final Dataset<Row> nightBuilding = meanDailyBuilding.filter(col("daily").equalTo(false)).withColumnRenamed("fullBuildingLocation", "fl").withColumnRenamed("day", "d").withColumnRenamed("avg_temp", "night_temp").withColumnRenamed("avg_hum", "night_hum");

        final Dataset<Row> diffBuilding = dailyBuilding.join(nightBuilding, nightBuilding.col("fl").equalTo(dailyBuilding.col("fullBuildingLocation")).and(nightBuilding.col("d").equalTo(dailyBuilding.col("day"))), "left_outer")
                .filter(col("d").isNotNull())
                .drop("daily", "d", "fl")
                .withColumn("temp_diff", expr("day_temp-night_temp"))
                .withColumn("hum_diff", expr("day_hum-night_hum"));
        diffBuilding.cache();


        //Daily night-day temperature difference - Neighborhood-scale
        final Dataset<Row> meanDailyNeighborhood = dataset
                .groupBy("daily", "day", "neighborhood")
                .agg(
                        avg("temperature").as("avg_temp"),
                        avg("humidity").as("avg_hum")
                )
                .orderBy("day");

        final Dataset<Row> dailyNeighborhood = meanDailyNeighborhood.filter(col("daily").equalTo(true)).withColumnRenamed("avg_temp", "day_temp").withColumnRenamed("avg_hum", "day_hum");
        final Dataset<Row> nightNeighborhood = meanDailyNeighborhood.filter(col("daily").equalTo(false)).withColumnRenamed("neighborhood", "fl").withColumnRenamed("day", "d").withColumnRenamed("avg_temp", "night_temp").withColumnRenamed("avg_hum", "night_hum");

        final Dataset<Row> diffNeighborhood = dailyNeighborhood.join(nightNeighborhood, nightNeighborhood.col("fl").equalTo(dailyNeighborhood.col("neighborhood")).and(nightNeighborhood.col("d").equalTo(dailyNeighborhood.col("day"))), "left_outer")
                .filter(col("d").isNotNull())
                .drop("daily", "d", "fl")
                .withColumn("temp_diff", expr("day_temp-night_temp"))
                .withColumn("hum_diff", expr("day_hum-night_hum"));
        diffNeighborhood.cache();


        //Month of the year with higher average night-day temperature difference - Room
        final Dataset<Row> avgMonthRoom = diffRoom.withColumn("month", month(col("day"))).groupBy("month")
                .agg(
                        avg("temp_diff").as("temp"),
                        avg("hum_diff").as("hum")
                )
                .orderBy("month");
        avgMonthRoom.cache();
        avgMonthRoom.show();
        final Dataset<Row> maxMonthRoom = avgMonthRoom.select(col("month"), col("temp")).where(col("temp").equalTo(avgMonthRoom.select(max("temp")).first().getDouble(0)));
        maxMonthRoom.show();
        
        //Month of the year with higher average night-day temperature difference - Building Level
        final Dataset<Row> avgMonthFloor = diffFloor.withColumn("month", month(col("day"))).groupBy("month")
                .agg(
                        avg("temp_diff").as("temp"),
                        avg("hum_diff").as("hum")
                )
                .orderBy("month");
        avgMonthFloor.cache();
        avgMonthFloor.show();
        final Dataset<Row> maxMonthFloor = avgMonthFloor.select(col("month"), col("temp")).where(col("temp").equalTo(avgMonthFloor.select(max("temp")).first().getDouble(0)));
        maxMonthFloor.show();


        //Month of the year with higher average night-day temperature difference - Building
        final Dataset<Row> avgMonthBuilding = diffBuilding.withColumn("month", month(col("day"))).groupBy("month")
                .agg(
                        avg("temp_diff").as("temp"),
                        avg("hum_diff").as("hum")
                )
                .orderBy("month");
        avgMonthBuilding.cache();
        avgMonthBuilding.show();
        final Dataset<Row> maxMonthBuilding = avgMonthBuilding.select(col("month"), col("temp")).where(col("temp").equalTo(avgMonthBuilding.select(max("temp")).first().getDouble(0)));
        maxMonthBuilding.show();


        //Month of the year with higher average night-day temperature difference - Neighborhood-scale
        final Dataset<Row> avgMonthNeighborhood = diffNeighborhood.withColumn("month", month(col("day"))).groupBy("month")
                .agg(
                        avg("temp_diff").as("temp"),
                        avg("hum_diff").as("hum")
                )
                .orderBy("month");
        avgMonthNeighborhood.cache();
        avgMonthNeighborhood.show();
        final Dataset<Row> maxMonthNeighborhood = avgMonthNeighborhood.select(col("month"), col("temp")).where(col("temp").equalTo(avgMonthNeighborhood.select(max("temp")).first().getDouble(0)));
        maxMonthNeighborhood.show();


        spark.close();

    }
}