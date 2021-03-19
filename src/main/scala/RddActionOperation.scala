import org.apache.spark.sql.SparkSession

object RddActionOperation {
  def main(args: Array[String]): Unit = {

    /** Q1:  create a SparkSession* */
    //Answer your question Q1 :
    val spark: SparkSession = SparkSession.builder().master("local").appName("5DOOP").getOrCreate()
    val sc = spark.sparkContext

    /**
     * You have a array with 5 words
     * Q2 : create a RDD called 'wordsRdd' from the wordsArray
     * */
    val wordsArray = Array("hello", "spark", "hello", "world", "hello")
    //Answer your question Q2 :
    val wordsRdd = sc.parallelize(wordsArray)
   // WordsRdd.foreach(println)   //pour afficher les résultats

    /**
     * Q3 : Now you have a wordsRdd with 5 words , then you need to return a new rdd from the wordsRdd
     * The new rdd returned must have the following key value pair data:
     * (hello,1)
     * (spark,1)
     * (hello,1)
     * (world,1)
     * (hello,1)
     */
    //Answer your question Q3 :
    val keys = sc.parallelize("1")
    val newWordsRdd = wordsRdd.cartesian(keys)
    // newWordsRdd.foreach(println)   //pour afficher les résultats

    /**
     * Q4 : You have 2 rdd following : rdd1 and rdd2
     * You need to do the following operations  :
     *      1. remove the duplicate elements
     *         2. do rdd union for rdd1 and rdd2
     *         3. do rdd intersection for rdd1 and rdd2
     * */
    val rdd1 = spark.sparkContext.parallelize(Array("coffe", "apple", "animal", "horse", "dog", "horse", "dog"))
    val rdd2 = spark.sparkContext.parallelize(Array("coffe", "cup", "world"))
    //Answer your question Q4 :
    val noDuplicateRdd1 = rdd1.distinct()
    val noDuplicateRdd2 = rdd2.distinct()
    val unionRdd = rdd1.union(rdd2)
    val intersectionRdd = rdd1.intersection(rdd2)


    /*
     //pour afficher les résultats Q4
      noDuplicateRdd1.foreach(println)
      noDuplicateRdd2.foreach(println)
      unionRdd.foreach(println)
      intersectionRdd.foreach(println)
     */
  }
}
