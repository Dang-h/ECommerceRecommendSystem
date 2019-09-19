package offline

import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ALSTrainer {

	def main(args: Array[String]): Unit = {

		val config = Map(
			"mongo.uri" -> "mongodb://sql:27017/recommender",
			"mongo.db" -> "recommender"
		)

		val conf: SparkConf = new SparkConf().setAppName("ALSTrainer").setMaster("local[*]")
		val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()

		val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

		import sparkSession.implicits._

		//加载评分数据
		val ratingRDD: RDD[Rating] = sparkSession
		  .read
		  .option("uri", mongoConfig.uri)
		  .option("collection", OfflineRecommender.MONGODB_RATING_COLLECTION)
		  .format("com.mongodb.spark.sql")
		  .load()
		  .as[ProductRating]
		  .rdd
		  .map(rating => Rating(rating.userId, rating.productId, rating.score))
		  .cache()
		ratingRDD

		//训练集的数据量为80%，测试集为20%


	}

}
