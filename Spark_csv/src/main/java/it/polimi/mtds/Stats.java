package it.polimi.mtds;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
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
                .withColumn("daily", hour(col("dateTime")).cast(DataTypes.IntegerType).lt(20).and(hour(col("dateTime")).cast(DataTypes.IntegerType).geq(8)));
        dataset.show();
        dataset.cache();





        //Hourly moving average - Room
        final Dataset<Row> movingAverageTemperatureAndHumidityHour = dataset
                .select(col("fullRoomLocation"), col("fullBuildingLocation"), col("fullFloorLocation"), col("neighborhood"), col("dateTime"), col("temperature"), col("humidity"));

        movingAverageTemperatureAndHumidityHour.cache();

        /*movingAverageTemperatureAndHumidityHour
                .write()
                .mode(SaveMode.Overwrite)
                .option("header", true)
                .option("delimiter", ";")
                .option("dateFormat","dd/MM/yyyy HH:mm:ss")
                .csv("../Stats.csv");*/

        movingAverageTemperatureAndHumidityHour.createOrReplaceTempView("hourMovingAverage");

        final Dataset<Row> movingAverageTemperatureAndHumidityHourRoom = spark.sql("SELECT fullRoomLocation, dateTime, temperature, avg(temperature) OVER (PARTITION BY fullRoomLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW) AS avg_temp, avg(humidity) OVER (PARTITION BY fullRoomLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW) AS avg_hum FROM hourMovingAverage");
        movingAverageTemperatureAndHumidityHourRoom.show();

        //Hourly moving average - Building Level
        final Dataset<Row> movingAverageTemperatureAndHumidityHourFloor = spark.sql("SELECT fullFloorLocation, dateTime, avg(temperature) OVER (PARTITION BY fullFloorLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW) AS avg_temp, avg(humidity) OVER (PARTITION BY fullRoomLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW) AS avg_hum FROM hourMovingAverage");
        movingAverageTemperatureAndHumidityHourFloor.show();

        //Hourly moving average - Building
        final Dataset<Row> movingAverageTemperatureAndHumidityHourBuilding = spark.sql("SELECT fullBuildingLocation, dateTime, avg(temperature) OVER (PARTITION BY fullBuildingLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW) AS avg_temp, avg(humidity) OVER (PARTITION BY fullRoomLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW) AS avg_hum FROM hourMovingAverage");
        movingAverageTemperatureAndHumidityHourBuilding.show();

        //Hourly moving average - Neighborhood-scale
        final Dataset<Row> movingAverageTemperatureAndHumidityHourNeighborhood = spark.sql("SELECT neighborhood, dateTime, avg(temperature) OVER (PARTITION BY neighborhood ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW) AS avg_temp, avg(humidity) OVER (PARTITION BY fullRoomLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW) AS avg_hum FROM hourMovingAverage");
        movingAverageTemperatureAndHumidityHourNeighborhood.show();

        //No more used, so removed from cache
        movingAverageTemperatureAndHumidityHour.unpersist();



        //Daily moving average - Room
        final Dataset<Row> movingAverageTemperatureAndHumidityDaily = dataset
                .select(col("fullRoomLocation"), col("fullBuildingLocation"), col("fullFloorLocation"), col("neighborhood"), col("dateTime"), col("temperature"), col("humidity"));

        movingAverageTemperatureAndHumidityHour.cache();
        movingAverageTemperatureAndHumidityDaily.createOrReplaceTempView("dailyMovingAverage");

        final Dataset<Row> movingAverageTemperatureAndHumidityDailyRoom = spark.sql("SELECT fullRoomLocation, dateTime, avg(temperature) OVER (PARTITION BY fullRoomLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) AS avg_temp, avg(humidity) OVER (PARTITION BY fullRoomLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) AS avg_hum FROM dailyMovingAverage");
        movingAverageTemperatureAndHumidityDailyRoom.show();

        //Daily moving average - Building Level
        final Dataset<Row> movingAverageTemperatureAndHumidityDailyFloor = spark.sql("SELECT fullFloorLocation, dateTime, avg(temperature) OVER (PARTITION BY fullFloorLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) AS avg_temp, avg(humidity) OVER (PARTITION BY fullRoomLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) AS avg_hum FROM dailyMovingAverage");
        movingAverageTemperatureAndHumidityDailyFloor.show();

        //Daily moving average - Building
        final Dataset<Row> movingAverageTemperatureAndHumidityDailyBuilding = spark.sql("SELECT fullBuildingLocation, dateTime, avg(temperature) OVER (PARTITION BY fullBuildingLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) AS avg_temp, avg(humidity) OVER (PARTITION BY fullRoomLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) AS avg_hum FROM dailyMovingAverage");
        movingAverageTemperatureAndHumidityDailyBuilding.show();

        //Daily moving average - Neighborhood-scale
        final Dataset<Row> movingAverageTemperatureAndHumidityDailyNeighborhood = spark.sql("SELECT neighborhood, dateTime, avg(temperature) OVER (PARTITION BY neighborhood ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) AS avg_temp, avg(humidity) OVER (PARTITION BY fullRoomLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) AS avg_hum FROM dailyMovingAverage");
        movingAverageTemperatureAndHumidityDailyNeighborhood.show();

        //No more used, so removed from cache
        movingAverageTemperatureAndHumidityDaily.unpersist();



        //Weekly moving average - Room
        final Dataset<Row> movingAverageTemperatureAndHumidityWeekly = dataset
                .select(col("fullRoomLocation"), col("fullBuildingLocation"), col("fullFloorLocation"), col("neighborhood"), col("dateTime"), col("temperature"), col("humidity"));

        movingAverageTemperatureAndHumidityWeekly.cache();
        movingAverageTemperatureAndHumidityWeekly.createOrReplaceTempView("weeklyMovingAverage");

        final Dataset<Row> movingAverageTemperatureAndHumidityWeeklyRoom = spark.sql("SELECT fullRoomLocation, dateTime, temperature, avg(temperature) OVER (PARTITION BY fullRoomLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 7 DAY PRECEDING AND CURRENT ROW) AS avg_temp, avg(humidity) OVER (PARTITION BY fullRoomLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 7 DAY PRECEDING AND CURRENT ROW) AS avg_hum FROM weeklyMovingAverage");
        movingAverageTemperatureAndHumidityWeeklyRoom.show();

        //Weekly moving average - Building Level
        final Dataset<Row> movingAverageTemperatureAndHumidityWeeklyFloor = spark.sql("SELECT fullFloorLocation, dateTime, avg(temperature) OVER (PARTITION BY fullFloorLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 7 DAY PRECEDING AND CURRENT ROW) AS avg_temp, avg(humidity) OVER (PARTITION BY fullRoomLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 7 DAY PRECEDING AND CURRENT ROW) AS avg_hum FROM weeklyMovingAverage");
        movingAverageTemperatureAndHumidityWeeklyFloor.show();

        //Weekly moving average - Building
        final Dataset<Row> movingAverageTemperatureAndHumidityWeeklyBuilding = spark.sql("SELECT fullBuildingLocation, dateTime, avg(temperature) OVER (PARTITION BY fullBuildingLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 7 DAY PRECEDING AND CURRENT ROW) AS avg_temp, avg(humidity) OVER (PARTITION BY fullRoomLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 7 DAY PRECEDING AND CURRENT ROW) AS avg_hum FROM weeklyMovingAverage");
        movingAverageTemperatureAndHumidityWeeklyBuilding.show();

        //Weekly moving average - Neighborhood-scale
        final Dataset<Row> movingAverageTemperatureAndHumidityWeeklyNeighborhood = spark.sql("SELECT neighborhood, dateTime, avg(temperature) OVER (PARTITION BY neighborhood ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 7 DAY PRECEDING AND CURRENT ROW) AS avg_temp, avg(humidity) OVER (PARTITION BY fullRoomLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 7 DAY PRECEDING AND CURRENT ROW) AS avg_hum FROM weeklyMovingAverage");
        movingAverageTemperatureAndHumidityWeeklyNeighborhood.show();

        //No more used, so removed from cache
        movingAverageTemperatureAndHumidityWeekly.unpersist();





        //Daily night-day temperature difference - Room
        final Dataset<Row> meanDailyRoom = dataset
                .groupBy("daily", "day", "fullRoomLocation")
                .agg(
                        avg("temperature").as("avg_temp"),
                        avg("humidity").as("avg_hum")
                )
                .orderBy("day");
        meanDailyRoom.cache();

        final Dataset<Row> dailyRoom = meanDailyRoom.filter(col("daily").equalTo(true)).withColumnRenamed("avg_temp", "day_temp").withColumnRenamed("avg_hum", "day_hum");
        final Dataset<Row> nightRoom = meanDailyRoom.filter(col("daily").equalTo(false)).withColumnRenamed("fullRoomLocation", "fl").withColumnRenamed("day", "d").withColumnRenamed("avg_temp", "night_temp").withColumnRenamed("avg_hum", "night_hum");

        //No more used, so removed from cache
        meanDailyRoom.unpersist();

        final Dataset<Row> diffRoom = dailyRoom.join(nightRoom, nightRoom.col("fl").equalTo(dailyRoom.col("fullRoomLocation")).and(nightRoom.col("d").equalTo(dailyRoom.col("day"))), "left_outer")
                .filter(col("d").isNotNull())
                .drop("daily", "d", "fl")
                .withColumn("temp_diff", expr("day_temp-night_temp"))
                .withColumn("hum_diff", expr("day_hum-night_hum"));
        diffRoom.cache();
        //diffRoom.show();



        //Daily night-day temperature difference - Floor Level
        final Dataset<Row> meanDailyFloor = dataset
                .groupBy("daily", "day", "fullFloorLocation")
                .agg(
                        avg("temperature").as("avg_temp"),
                        avg("humidity").as("avg_hum")
                )
                .orderBy("day");
        meanDailyFloor.cache();

        final Dataset<Row> dailyFloor = meanDailyFloor.filter(col("daily").equalTo(true)).withColumnRenamed("avg_temp", "day_temp").withColumnRenamed("avg_hum", "day_hum");
        final Dataset<Row> nightFloor = meanDailyFloor.filter(col("daily").equalTo(false)).withColumnRenamed("fullFloorLocation", "fl").withColumnRenamed("day", "d").withColumnRenamed("avg_temp", "night_temp").withColumnRenamed("avg_hum", "night_hum");

        //No more used, so removed from cache
        meanDailyFloor.unpersist();

        final Dataset<Row> diffFloor = dailyFloor.join(nightFloor, nightFloor.col("fl").equalTo(dailyFloor.col("fullFloorLocation")).and(nightFloor.col("d").equalTo(dailyFloor.col("day"))), "left_outer")
                .filter(col("d").isNotNull())
                .drop("daily", "d", "fl")
                .withColumn("temp_diff", expr("day_temp-night_temp"))
                .withColumn("hum_diff", expr("day_hum-night_hum"));
        diffFloor.cache();
        //diffFloor.show();



        //Daily night-day temperature difference - Building Level
        final Dataset<Row> meanDailyBuilding = dataset
                .groupBy("daily", "day", "fullBuildingLocation")
                .agg(
                        avg("temperature").as("avg_temp"),
                        avg("humidity").as("avg_hum")
                )
                .orderBy("day");
        meanDailyBuilding.cache();

        final Dataset<Row> dailyBuilding = meanDailyBuilding.filter(col("daily").equalTo(true)).withColumnRenamed("avg_temp", "day_temp").withColumnRenamed("avg_hum", "day_hum");
        final Dataset<Row> nightBuilding = meanDailyBuilding.filter(col("daily").equalTo(false)).withColumnRenamed("fullBuildingLocation", "fl").withColumnRenamed("day", "d").withColumnRenamed("avg_temp", "night_temp").withColumnRenamed("avg_hum", "night_hum");

        //No more used, so removed from cache
        meanDailyBuilding.unpersist();

        final Dataset<Row> diffBuilding = dailyBuilding.join(nightBuilding, nightBuilding.col("fl").equalTo(dailyBuilding.col("fullBuildingLocation")).and(nightBuilding.col("d").equalTo(dailyBuilding.col("day"))), "left_outer")
                .filter(col("d").isNotNull())
                .drop("daily", "d", "fl")
                .withColumn("temp_diff", expr("day_temp-night_temp"))
                .withColumn("hum_diff", expr("day_hum-night_hum"));
        diffBuilding.cache();
        //diffBuilding.show();



        //Daily night-day temperature difference - Neighborhood-scale
        final Dataset<Row> meanDailyNeighborhood = dataset
                .groupBy("daily", "day", "neighborhood")
                .agg(
                        avg("temperature").as("avg_temp"),
                        avg("humidity").as("avg_hum")
                )
                .orderBy("day");
        meanDailyNeighborhood.cache();

        final Dataset<Row> dailyNeighborhood = meanDailyNeighborhood.filter(col("daily").equalTo(true)).withColumnRenamed("avg_temp", "day_temp").withColumnRenamed("avg_hum", "day_hum");
        final Dataset<Row> nightNeighborhood = meanDailyNeighborhood.filter(col("daily").equalTo(false)).withColumnRenamed("neighborhood", "fl").withColumnRenamed("day", "d").withColumnRenamed("avg_temp", "night_temp").withColumnRenamed("avg_hum", "night_hum");

        //No more used, so removed from cache
        meanDailyNeighborhood.unpersist();

        final Dataset<Row> diffNeighborhood = dailyNeighborhood.join(nightNeighborhood, nightNeighborhood.col("fl").equalTo(dailyNeighborhood.col("neighborhood")).and(nightNeighborhood.col("d").equalTo(dailyNeighborhood.col("day"))), "left_outer")
                .filter(col("d").isNotNull())
                .drop("daily", "d", "fl")
                .withColumn("temp_diff", expr("day_temp-night_temp"))
                .withColumn("hum_diff", expr("day_hum-night_hum"));
        diffNeighborhood.cache();
        //diffNeighborhood.show();





        //Month of the year with higher average night-day temperature difference - Room
        final Dataset<Row> avgMonthRoom = diffRoom.withColumn("month", month(col("day"))).groupBy("month")
                .agg(
                        avg("temp_diff").as("temp"),
                        avg("hum_diff").as("hum")
                )
                .orderBy("month");
        avgMonthRoom.cache();
        //avgMonthRoom.show();

        //No more used, so removed from cache
        diffRoom.unpersist();

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
        //avgMonthFloor.show();

        //No more used, so removed from cache
        diffFloor.unpersist();

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
        //avgMonthBuilding.show();

        //No more used, so removed from cache
        diffBuilding.unpersist();

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
        //avgMonthNeighborhood.show();

        //No more used, so removed from cache
        diffNeighborhood.unpersist();

        final Dataset<Row> maxMonthNeighborhood = avgMonthNeighborhood.select(col("month"), col("temp")).where(col("temp").equalTo(avgMonthNeighborhood.select(max("temp")).first().getDouble(0)));
        maxMonthNeighborhood.show();

        //writeToCsv(maxMonthNeighborhood);
        /*maxMonthNeighborhood
                .write()
                .mode(SaveMode.Overwrite)
                .option("header", true)
                .option("delimiter", ";")
                .option("dateFormat","dd/MM/yyyy HH:mm:ss")
                .csv("../Stats");*/

        spark.close();

    }

    /*public static void writeToCsv(Dataset<Row> dataset) {
        dataset
                .write()
                .mode(SaveMode.Overwrite)
                .option("header", true)
                .option("delimiter", ";")
                .option("dateFormat","dd/MM/yyyy HH:mm:ss")
                .csv("../Stats");
    }*/
}