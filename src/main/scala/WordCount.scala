import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object HelloWorld {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
      setMaster("local").
      setAppName("LearnScalaSpark")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    // Load our input data.
    val input =  sc.textFile("src/main/resources/book.txt")
    // Split up into words.
    val words = input.flatMap(line => line.split(" ")).map(word => word.toLowerCase)
    // Transform into word and count.
    val counts = words.map(word => (word, 1)).reduceByKey(_ + _).sortBy(_._2,false)
    // Save the word count back to csv file and cut TOP =100.
//    counts.saveAsTextFile("src/main/resources/result")
    val spark = SparkSession.builder.config(conf).getOrCreate()
    val df = spark.createDataFrame(counts).toDF("word", "amount")
    val result = df.limit(100)

    result.write.format("csv").save("src/main/resources/result")
  }
}