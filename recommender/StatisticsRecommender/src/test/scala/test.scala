import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object test {

	def main(args: Array[String]): Unit = {

		val sparkConf: SparkConf = new SparkConf().setAppName("test").setMaster("local[*]")

		val sparkContext = new SparkContext(sparkConf)

		val rdd: RDD[Int] = sparkContext.makeRDD(Array(1, 2, 3, 4, 5))

		val mapPartitionWithIndex: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex((index, items) => (items.map((index, _))))

		mapPartitionWithIndex.foreach(println)
	}

}
