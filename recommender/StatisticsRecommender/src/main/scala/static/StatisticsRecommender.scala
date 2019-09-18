package static

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}


case class Product(productId: Int, name: String, categories: String, imageUrl: String, tags: String)

case class Rating(userId: Int, productId: Int, score: Double, timestamp: Long)

case class MongoConfig(uri: String, db: String)

//TODO 推介给用户的商品的id，r为推介给用户的商品用户对该商品的评分
case class Recommendation(rid: Int, r: Double)


object StatisticsRecommender {

	val MONGODB_PRODUCT_COLLECTION = "Product"
	val MONGODB_RATING_COLLECTION = "Rating"

	//需要统计的指标的表名
	//热门历史商品
	val RATE_MORE_PRODUCTS = "RateMoreProduct"
	//最近热门历史商品
	val RATE_MORE_RECENTLY_PRODUCT = "RateMoreRecentlyProduct"
	//商品平均得分
	val AVERAGE_PRODUCT = "averageProduct"

	def main(args: Array[String]): Unit = {

		val config = Map(
			"mongo.uri" -> "mongodb://localhost:27017/recommender",
			"mongo.db" -> "recommender"
		)

		//创建sparkConf
		val sparkConf: SparkConf = new SparkConf().setAppName("statisticsRecommender").setMaster("local[*]")

		//创建sparkSession
		val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

		//MongoDB配置
		val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

		import spark.implicits._

		//加载数据数据库数据
		val ratingDF: DataFrame = spark.read.option("uri", mongoConfig.uri).option("collection", MONGODB_RATING_COLLECTION)
		  .format("com.mongodb.spark.sql").load()
		  .as[Rating].toDF()

		val productDF: DataFrame = spark.read.option("uri", mongoConfig.uri).option("collection", MONGODB_PRODUCT_COLLECTION)
		  .format("com.mongodb.spark.sql").load()
		  .as[Product].toDF()

		//热门商品
		//创建临时表
		ratingDF.createOrReplaceTempView("ratings")

		val rateMoreProductsDF: DataFrame = spark.sql("select productId, count(productId) as count from ratings group by productId")

		rateMoreProductsDF.write.option("uri", mongoConfig.uri).option("collection", RATE_MORE_PRODUCTS)
		  .mode("overwrite").format("com.mongodb.spark.sql").save()

		//最近热门商品
		//格式化时间
		val dateFormat = new SimpleDateFormat("yyyyMM")

		spark.udf
		  .register("changeDate", (date: Long) => dateFormat.format(new Date(date * 1000L)).toInt)

	}

}
