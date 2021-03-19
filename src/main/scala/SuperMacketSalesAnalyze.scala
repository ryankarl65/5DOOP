import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.TimestampType

object SuperMacketSalesAnalyze {

  def main(args: Array[String]): Unit = {

    /**
     * Please answer your question using the csv file supermarket_sales.csv which is in the project folder : /spark-warehouse
     * You have a description file for the data source :  /Data_Description.txt
     *
     */

    /** Q1:  create a SparkSession* */
    //Answer your question Q1 :
    val spark: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("5DOOP")
      .getOrCreate()
    val sc = spark.sparkContext


    /** Q2: create a DataFrame object based on the csv file : supermarket_sales * */
    //Answer your question Q2 :
    val df = spark.read
      .option("header",value = true)
      .option("delimiter",",")
      .csv("spark-warehouse/supermarket_sales.csv")


    /**
     * Use DataFrame for this question
     * Q3 : display the first 5 lines data using DataFrame
     */
    //Answer your question Q3 :
    df.show(5)


    /**
     * Use DataFrame for this question
     * Q4 : count the number of lines that have the rating less than 2
     */
    //Answer your question Q4 :
   val countRatingLess2 = df.where("Rating<2").count()
    println(countRatingLess2)

    /**
     * Q5 : find the line that have the Rating more than 9 and order them by rating and branch
     * */
    //Answer your question Q5 :
    df.where("Rating>9")
      .orderBy(col("Rating"),col("Branch"))
      .show()


    /**
     * Use DataFrame for this question
     * Q6 : order the sales data by Date using DataFrame
     */
    //Answer your question Q6 :
    df.orderBy("Date").show()

    /**
     * Use SparkSql or DataFrame for this question
     * Q7 :Find Invoice ID, Branch , City, Unit , price, Quantity, Date, Rating form the sales data
     * You need to convert the 'Date' column's format from String to Date and reorder them by Date column
     */
    //Answer your question Q7 :
    val dfQ7 = df.withColumn("Date", unix_timestamp(col("Date"),"MM/dd/yyyy").cast(TimestampType))
    dfQ7.select("Invoice ID","Branch","City","Unit","price","Quantity","Date","Rating").orderBy("Date").show()

    /**
     * Q8 : find the largest and smallest Rating for every city
     * Use DataFrame or SparkSql for this question
     * */
    //Answer your question Q8 :

    //largest Rating
    df.groupBy("City")
      .agg(max("Rating"))
      .show()

    //Smallest Rating
    df.groupBy("City")
      .agg(min("Rating"))
      .show()



    /** Stop the spark session */
    spark.stop()
  }
}
