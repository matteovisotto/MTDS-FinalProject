import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import utils.LogUtils;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.max;

/**
 * Bank example
 *
 * Input: csv files with list of deposits and withdrawals, having the following
 * schema ("person: String, account: String, amount: Int)
 *
 * Queries
 * Q1. Print the total amount of withdrawals for each person.
 * Q2. Print the person with the maximum total amount of withdrawals
 * Q3. Print all the accounts with a negative balance
 *
 * The code exemplifies the use of SQL primitives.  By setting the useCache variable,
 * one can see the differences when enabling/disabling cache.
 */
public class Bank {
    public static void main(String[] args) {
        LogUtils.setLogLevel();

        final String master = args.length > 0 ? args[0] : "local[4]";
        final String filePath = args.length > 1 ? args[1] : "./";
        final String appName = "Stats";

        final SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName(appName)
                .getOrCreate();

        final List<StructField> mySchemaFields = new ArrayList<>();
        mySchemaFields.add(DataTypes.createStructField("location", DataTypes.StringType, true));
        mySchemaFields.add(DataTypes.createStructField("dateTime", DataTypes.DateType, true));
        mySchemaFields.add(DataTypes.createStructField("temperature", DataTypes.FloatType, true));
        mySchemaFields.add(DataTypes.createStructField("humidity", DataTypes.FloatType, true));
        final StructType mySchema = DataTypes.createStructType(mySchemaFields);

        final Dataset<Row> dataset = spark
                .read()
                .option("header", "false")
                .option("delimiter", ";")
                .schema(mySchema)
                .csv(filePath + "files/dataset.csv");

        // Used in two different queries
        dataset.cache();

        //Hourly moving average - Room
        final Dataset<Row> movingAverageTemperature = dataset
                .groupBy("location")
                .avg("temperature")
                .as("sumTemperature");

        // Used in two different queries
        movingAverageTemperature.cache();

        movingAverageTemperature.show();

        /*final Dataset<Row> sumHumidity = dataset
                .groupBy("location")
                .sum("humidity")
                .as("sumHumidity");
                //.select("location", "sum(humidity)");

        // Used in two different queries
        if (useCache) {
            sumHumidity.cache();
        }

        sumHumidity.show();*/



        //Hourly moving average - Building



        //Hourly moving average - Building Level



        //Hourly moving average - Neighborhood-scale



        //Daily moving average - Room



        //Daily moving average - Building



        //Daily moving average - Building Level



        //Daily moving average - Neighborhood-scale



        //Weekly moving average - Room



        //Weekly moving average - Building Level



        //Weekly moving average - Building



        //Weekly moving average - Neighborhood-scale



        //Hourly moving average - Neighborhood-scale



        //Day temperature -> average temperature between 8am and 8pm
        //Night temperature -> average temperature between 8pm and 8am
        //Daily night-day temperature difference - Room



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