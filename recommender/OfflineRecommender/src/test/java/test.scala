import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//统计出每一个省份广告被点击次数的 TOP3
//时间戳，省份，城市，用户，广告
object test {

	def main(args: Array[String]): Unit = {
		val conf: SparkConf = new SparkConf().setAppName("test").setMaster("local[*]")
		val sc = new SparkContext(conf)

		val dataRDD: RDD[String] = sc.textFile("F:\\workSpace\\ECommerceRecommendSystem\\recommender\\OfflineRecommender\\src\\test\\input\\agent.log")

		//((province, Ad), 1)
		val provinceAdTo1RDD: RDD[((String, String), Int)] = dataRDD.map {
			lines =>
				val splitData: Array[String] = lines.split(" ")
				((splitData(1), splitData(4)), 1)
		}
		//		provinceAdTo1RDD.foreach(println)

		//((province, Ad), sum),每个省每个广告被点击的总数
		val provinceAdToSum: RDD[((String, String), Int)] = provinceAdTo1RDD.reduceByKey(_ + _)
		//		provinceAdToSum.foreach(println)

		//(province, (Ad, sumOfAd))
		val adSum: RDD[(String, (String, Int))] = provinceAdToSum.map(items => (items._1._1, (items._1._2, items._2)))

		//同一省份聚合(province, Iterator((Ad1, sumOfAd1), (Ad2, sumOfAd2), ...))
		val groupByProvince: RDD[(String, Iterable[(String, Int)])] = adSum.groupByKey()

		//根据广告点击数排序，取前3
		val top3: RDD[(String, List[(String, Int)])] = groupByProvince.mapValues(
			items => items.toList.sortWith((item1, item2) => item1._2 > item2._2).take(3))

		top3.collect().foreach(println)

		sc.stop()
	}
}
