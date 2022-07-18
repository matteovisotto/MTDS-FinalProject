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

        dataset = dataset.withColumn("neighborhoods",split(col("location"),"[.]").getItem(0))
                .withColumn("building",split(col("location"),"[.]").getItem(1))
                .withColumn("floor",split(col("location"),"[.]").getItem(2))
                .withColumn("room",split(col("location"),"[.]").getItem(3))
                .withColumnRenamed("location", "fullLocation")
                .withColumn("hour", hour(col("dateTime")))
                .withColumn("day", to_date(col("dateTime")))
                .withColumn("week", weekofyear(col("dateTime")))
                .withColumn("month", month(col("dateTime")))
                .withColumn("year", year(col("dateTime")))
                .withColumn("night", hour(col("dateTime")).cast(DataTypes.IntegerType).lt(8).or(hour(col("dateTime")).cast(DataTypes.IntegerType).geq(20)))
                .withColumn("daily", hour(col("dateTime")).cast(DataTypes.IntegerType).lt(20).and(hour(col("dateTime")).cast(DataTypes.IntegerType).geq(8)))
                .withColumn("rolling_average", avg("temperature").over(w));
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

        movingAverageTemperatureAndHumidityHour.show();
        //SU QUESTA MANCA ROOM MENTRE SU QUELLA SOTTO OK BUILDING MA NON RAGGRUPPATO PER GIORNO.


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

        movingAverageTemperatureAndHumidityDaily.show();


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

        movingAverageTemperatureAndHumidityWeekly.show();


        //Weekly moving average - Building Level



        //Weekly moving average - Building



        //Weekly moving average - Neighborhood-scale



        //Hourly moving average - Neighborhood-scale



        //Day temperature -> average temperature between 8am and 8pm
        //Night temperature -> average temperature between 8pm and 8am
        //Daily night-day temperature difference - Room
        final Dataset<Row> meanDaily = dataset
                .groupBy("daily", "day", "room")
                .agg(
                        avg("temperature").as("avg_temp"),
                        avg("humidity").as("avg_hum")
                )
                .orderBy("day");
                /*.rdd()
                .zipWithIndex()
                .foreach((a,b) -> {

                });*/

        meanDaily.cache();
        meanDaily.show();

        //Daily night-day temperature difference - Building Level



        //Daily night-day temperature difference - Building



        //Daily night-day temperature difference - Neighborhood-scale



        //Month of the year with higher average night-day temperature difference - Room



        //Month of the year with higher average night-day temperature difference - Building Level



        //Month of the year with higher average night-day temperature difference - Building



        //Month of the year with higher average night-day temperature difference - Neighborhood-scale






        /*final long maxTotal = sumWithdrawals
                .agg(max("sum(amount)"))
                .first()
                .getLong(0);

        final Dataset<Row> maxWithdrawals = sumWithdrawals
                .filter(sumWithdrawals.col("sum(amount)").equalTo(maxTotal));

        maxWithdrawals.show();

        // Q3 Accounts with negative balance

        final Dataset<Row> totWithdrawals = withdrawals
                .groupBy("account")
                .sum("amount")
                .drop("person")
                .as("totalWithdrawals");

        final Dataset<Row> totDeposits = deposits
                .groupBy("account")
                .sum("amount")
                .drop("person")
                .as("totalDeposits");

        final Dataset<Row> negativeAccounts = totWithdrawals
                .join(totDeposits, totDeposits.col("account").equalTo(totWithdrawals.col("account")), "left_outer")
                .filter(totDeposits.col("sum(amount)").isNull().and(totWithdrawals.col("sum(amount)").gt(0)).or
                                (totWithdrawals.col("sum(amount)").gt(totDeposits.col("sum(amount)")))
                ).select(totWithdrawals.col("account"));

        negativeAccounts.show();*/

        spark.close();

    }
}