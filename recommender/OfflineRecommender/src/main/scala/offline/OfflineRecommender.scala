package offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix


case class Product(productId: Int, name: String, categories: String, imageUrl: String, tags: String)

case class ProductRating(userId: Int, productId: Int, score: Double, timestamp: Long)

case class MongoConfig(uri: String, db: String)

//rid为推介给用户的商品的id，r为推介给用户的商品用户对该商品的评分
case class Recommendation(rid: Int, r: Double)

case class UserRecs(userId: Int, recs: Seq[Recommendation])

case class ProductRecs(productId: Int, recs: Seq[Recommendation])


object OfflineRecommender {

	val MONGODB_RATING_COLLECTION = "Rating"
	val MONGODB_PRODUCT_COLLECTION = "Products"

	val USER_MAX_RECOMMENDATION = 20

	val USER_RECS = "UserRecs"
	val PRODUCT_RECS = "ProductRecs"

	def main(args: Array[String]): Unit = {

		val config = Map(
			"mongo.uri" -> "mongodb://sql:27017/recommender",
			"mongo.db" -> "recommender"
		)

		val sparkConf: SparkConf = new SparkConf().setAppName("offlineRecommender").setMaster("local[*]")
		  .set("spark.executor.memory", "6G")
		  .set("spark.driver.memory", "2G")

		val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

		val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

		import sparkSession.implicits._

		//读取业务数据，转换成RDD
		val ratingRDD: RDD[ProductRating] = sparkSession.read.option("uri", mongoConfig.uri)
		  .option("collection", MONGODB_RATING_COLLECTION)
		  .format("com.mongodb.spark.sql")
		  .load()
		  .as[ProductRating]
		  .rdd

		//拿出用户ID，产品ID和评分数据，TODO ETL去掉时间戳并缓存
		val ratingsRDD: RDD[(Int, Int, Double)] = ratingRDD.map(ratings => (ratings.userId, ratings.productId, ratings.score)).cache()

		//抽取userId， 去重
		val userRDD: RDD[Int] = ratingsRDD.map(_._1).distinct()

		//读取商品数据集
		val productRDD: RDD[Product] = sparkSession.read.option("uri", mongoConfig.uri)
		  .option("collection", MONGODB_PRODUCT_COLLECTION)
		  .format("com.mongodb.spark.sql")
		  .load()
		  .as[Product]
		  .rdd

		//抽取产品ID，缓存
		val productsRDD: RDD[Int] = productRDD.map(_.productId).cache()

		//创建训练数据集
		val trainData: RDD[Rating] = ratingsRDD.map(x => Rating(x._1, x._2, x._3))
		val (rank, iterations, lambda) = (50, 5, 0.01)

		//训练ALS模型
		val model: MatrixFactorizationModel = ALS.train(trainData, rank, iterations, lambda)

		//计算用户推介矩阵

		val userProducts: RDD[(Int, Int)] = userRDD.cartesian(productsRDD)
		//预测评分
		val preRatings: RDD[Rating] = model.predict(userProducts)

		val userRecs: DataFrame = preRatings.filter(_.rating > 0)
		  .map((r: Rating) => (
			r.user,
			(r.product, r.rating)
		  ))
		  .groupByKey()
		  .map {
			  case (userId, recs) => UserRecs(
				  userId,
				  recs.toList.sortWith(_._2 > _._2)
					.take(USER_MAX_RECOMMENDATION)
					.map(x => Recommendation(x._1, x._2))
			  )
		  }.toDF()

		userRecs.write.option("uri", mongoConfig.uri)
		  .option("collection", USER_RECS)
		  .mode("overwrite")
		  .format("com.mongodb.spark.sql").save()

		//商品隐特征矩阵
		val productFeatures: RDD[(Int, DoubleMatrix)] = model.productFeatures
		  .map { case (productId, features) => (productId, new DoubleMatrix(features)) }

		//自己和自己做笛卡尔积：自连接
		val productRecs: DataFrame = productFeatures.cartesian(productFeatures)
		  .filter { case (a, b) => a._1 != b._1 }
		  .map { case (a, b) =>
			  //计算余弦相似度
			  val simScore: Double = this.consinSim(a._2, b._2)
			  (a._1, (b._1, simScore))
		  }
		  .groupByKey()
		  .map { case (productId, items) => ProductRecs(productId,
			  items.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2)))
		  }
		  .toDF()

		productRecs.write.option("uri", mongoConfig.uri).option("collection", PRODUCT_RECS)
		  .mode("overwrite").format("com.mongodb.spark.sql")
		  .save()

		sparkSession.close()

	}

	def consinSim(product1: DoubleMatrix, product2: DoubleMatrix): Double = {
		//两个向量的内积/
		product1.dot(product2) / (product1.norm2() * product2.norm2())
	}
}
