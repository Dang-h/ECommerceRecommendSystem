package dataLoader

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI, MongoCollection}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

//0 3982
//1 ^Fuhlen 富勒 M8眩光舞者时尚节能无线鼠标(草绿)(眩光.悦动.时尚炫舞鼠标 12个月免换电池 高精度光学寻迹引擎 超细微接收器10米传输距离)
//2 ^1057,439,736
//3 ^B009EJN4T2
//4 ^https://images-cn-4.ssl-images-amazon.com/images/I/31QPvUDNavL._SY300_QL70_.jpg
//5 ^外设产品|鼠标|电脑/办公
//6 ^富勒|鼠标|电子产品|好用|外观漂亮

// products 数据格式：productId,name,categoryIds, amazonId, imageUrl, categories, tags
// 提取5个字段：productId,name,categories，imageUrl，tags
case class Product(productId: Int, name: String, categories: String, imageUrl: String, tags: String)

// ratings 数据格式：userId,prudcutId,rating,timestamp
case class Ratins(userId: Int, productId: Int, score: Double, timestamp: Long)

//Mongo配置
case class MongoConfig(uri: String, db: String)


object DataLoader {

	val PRODUCT_DATA_PATH = "F:\\workSpace\\ECommerceRecommendSystem\\recommender\\DataLoader\\src\\main" +
	  "\\resources\\products.csv"
	val RATING_DATA_PATH = "F:\\workSpace\\ECommerceRecommendSystem\\recommender\\DataLoader\\src\\main" +
	  "\\resources\\ratings.csv"

	val MONGODB_PRODUCT_COLLECTION = "Product"
	val MONGODB_RATING_COLLECTION = "Rating"


	def main(args: Array[String]): Unit = {

		val config: Map[String, String] = Map(
			"mongo.uri" -> "mongodb://sql:27017/recommender",
			"mongo.db" -> "recommender"
		)

		//创建sparkConf
		val sparkConf: SparkConf = new SparkConf().setAppName("DataLoader").setMaster("local[*]")

		//为了使用SparkSQL创建SparkSession
		val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

		import sparkSession.implicits._

		//加载数据集
		val productRDD: RDD[String] = sparkSession.sparkContext.textFile(PRODUCT_DATA_PATH)

		//RDD转DataFrame
		val productDF: DataFrame = productRDD.map(item => {
			val splitArr: Array[String] = item.split("\\^")
			Product(splitArr(0).toInt, splitArr(1).trim, splitArr(5).trim, splitArr(4).trim, splitArr(6).trim)
		}).toDF()

		//				productDF.show()

		val ratingRDD: RDD[String] = sparkSession.sparkContext.textFile(RATING_DATA_PATH)
		val ratingDF: DataFrame = ratingRDD.map(item => {
			val splitArr: Array[String] = item.split(",")
			Ratins(splitArr(0).toInt, splitArr(1).toInt, splitArr(2).toDouble, splitArr(3).toLong)
		}).toDF()

//				ratingDF.show()

		//声明一个隐式配置对象
		val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

		//数据存入MongoDB
		storeDataInMongoDB(productDF, ratingDF, mongoConfig)

		//关闭spark
		sparkSession.stop()

	}


	//函数柯里化
	def storeDataInMongoDB(productDF: DataFrame, ratingDF: DataFrame,mongoConfig: MongoConfig): Unit = {

		//新建MongoDB连接
		val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

		//通过MongoDB客户端拿到表的操作对象;如果表存在就将其删除
		val productCollection = mongoClient(mongoConfig.db)(MONGODB_PRODUCT_COLLECTION)
		productCollection.dropCollection()

		val ratingCollection = mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)
		ratingCollection.dropCollection()

		//将数据写入MongoDB
		productDF.write.option("uri", mongoConfig.uri)
		  .option("collection", MONGODB_PRODUCT_COLLECTION)
		  .mode("overwrite")
		  .format("com.mongodb.spark.sql")
		  .save()

		ratingDF.write.option("uri", mongoConfig.uri)
		  .option("collection", MONGODB_RATING_COLLECTION)
		  .mode("overwrite")
		  .format("com.mongodb.spark.sql").save()

		//对表建索引，1为升序索引；查询频繁的字段才需建立索引
		productCollection.createIndex(MongoDBObject("productId" -> 1))
		ratingCollection.createIndex(MongoDBObject("userId" -> 1))
		ratingCollection.createIndex(MongoDBObject("productId" -> 1))

		//关闭连接
		mongoClient.close()

	}
}
