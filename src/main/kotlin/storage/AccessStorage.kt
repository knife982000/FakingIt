package storage

import edu.isistan.fakenews.storage.MongoDBStorage
import edu.isistan.fakenews.storage.DEBUG_DB
import javax.imageio.ImageIO
import java.io.ByteArrayInputStream
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import com.mongodb.client.model.Filters
import org.bson.types.ObjectId
import java.util.function.Consumer
import org.bson.Document
import com.mongodb.client.MongoClients
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.LocalDateTime
import java.time.Period
import java.time.Instant
import java.time.ZoneId
import java.time.LocalDate
import edu.isistan.fakenews.crawler.TwitterCrawler

import edu.isistan.fakenews.*

class AccessStorage

var LOGGER = LoggerFactory.getLogger(AccessStorage::class.java)!!

fun findScreenshot(storage : MongoDBStorage) {


	storage.tweets.find().forEach{
		println(it.tweetId)
		//			println(it.text)

		if(storage.findTweetScreenshot(it.tweetId) != null){

			println("saving...")

			val file = storage.screenshotFS.openDownloadStream(it.tweetId.toString())
			val byte_array = file.readBytes()

			val input_stream= ByteArrayInputStream(byte_array);
			val final_buffered_image = ImageIO.read(input_stream);
			ImageIO.write(final_buffered_image , "png", File("${it.tweetId}.png") );

			val fileE = storage.tweetScreenshotFS.openDownloadStream(it.tweetId.toString())
					val byte_arrayE = fileE.readBytes()

					val input_streamE= ByteArrayInputStream(byte_arrayE);
			val final_buffered_imageE = ImageIO.read(input_streamE);
			ImageIO.write(final_buffered_imageE , "png", File("${it.tweetId}_obfuscated.png") );

		}
	}


}

fun duplicates(storage : MongoDBStorage){

	val tweets = HashSet<Long>()
			val repetidos = HashSet<Long>()

			storage.tweetFavorites.find().forEach{

		if(tweets.contains(it.tweetId))
			repetidos.add(it.tweetId)
			else
				tweets.add(it.tweetId)
	}

	val to_remove = HashSet<Any>()

			repetidos.forEach{

		var max = Integer.MAX_VALUE
				var which_min : Any? = null
				storage.tweetFavorites.find(Filters.eq("tweetId", it)).		
				forEach{

			if(it.users.size < max){
				max = it.users.size
						which_min = it.id
			}

		}

		if(which_min != null)
			to_remove.add(which_min!!)		
	}

	println("Total duplicados "+repetidos.size)
	println("Total to remove "+to_remove.size)

	to_remove.forEach{
		storage.tweetFavorites.deleteOne(Filters.eq("_id",it))
		println(storage.tweetFavorites.find().toHashSet().size)
	}


}

fun getJson(){
	var client = MongoClients.create()
			var database = client.getDatabase("FakeNewsTest")
			var collection = database.getCollection("tweets")
			for (doc in collection.find()) {
				println(doc.toJson())
			}
	client.close()
}

fun checkInconsistencies(path : String, storage : MongoDBStorage){

	val file = File(path)
			val tweetIds = ArrayList<Long>()
			file.readLines().map { it.trim() }.
			filter { it.isNotEmpty() }.
			forEach {
				tweetIds.add(it.toLong())	
			}

			val writer = Files.newBufferedWriter(File("missing_tweets.txt").toPath());
//			tweetIds.filter{storage.findTweet(it) == null}.forEach{
//
//				writer.write(it.toString())
//				writer.newLine()	
//
//			}
	
			tweetIds.forEach{
				val tweet = storage.findTweet(it)
				if(tweet != null)
					if (storage.findUser(tweet.userId) == null){
						writer.write(it.toString())
						writer.newLine()	
					}
			}
	
			writer.close()
}

//a las XX hs de bajado un tweet analizar las reacciones, como para dar tiempo a que se genere algo...
fun checkAndProcessTweets(storage : MongoDBStorage){
	
	val tweetCrawler = TwitterCrawler(storage)
	
	while(storage.tweets.find().asSequence().map{it.tweetId}.find{storage.findReplies(it) == null} != null)
		storage.tweets.find(Filters.lt("created",LocalDateTime.now().minusHours(7))).asSequence().map{it.tweetId}.chunked(3).forEach{
			tweetCrawler.run(it)
		} 
	
//	while(storage.tweets.find().asSequence().map{it.tweetId}.find{storage.findReplies(it) == null} != null)
//		tweetCrawler.run(storage.tweets.find().filter{
//			Duration.between(it.created.toInstant().atZone(ZoneId.systemDefault())
//      .toLocalDateTime(), LocalDateTime.now()).toHours() > 7
//	}.map{it.tweetId}.toMutableList())
						
}
	


fun main(args: Array<String>){
	
	DEBUG_DB = true
	val storage = MongoDBStorage()

	configure("settings.properties")
	
//	configure(args[0])
	
	checkAndProcessTweets(storage)
	
//			checkInconsistencies("ids_teleton.txt",storage)

	storage.close()
}

