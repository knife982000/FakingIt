package storage

import edu.isistan.fakenews.storage.MongoDBStorage
import edu.isistan.fakenews.storage.DEBUG_DB
import javax.imageio.ImageIO
import java.io.ByteArrayInputStream
import java.io.File

fun main(){
		DEBUG_DB=true
		val storage = MongoDBStorage()
	
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

