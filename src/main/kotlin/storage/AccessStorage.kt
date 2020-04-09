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

fun main(){
		DEBUG_DB=true
		val storage = MongoDBStorage()
	
//		val writer = Files.newBufferedWriter(File("ids_teleton.txt").toPath());
//		var i = 0
//		storage.tweets.find().forEach{
////			println(it.tweetId)
//			writer.write(it.tweetId.toString())
//			writer.newLine()
//			if ( i % 100 == 0)
//				println(i)
//			i++
//		}
//		writer.close()
	
	duplicates(storage)
	
//	storage.tweets.find().forEach{
//		if(storage.findReactions(it.tweetId,"favorites") == null)
//			println(it.tweetId)
//	}
	
}

