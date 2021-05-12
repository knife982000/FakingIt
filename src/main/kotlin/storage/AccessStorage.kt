package storage

import edu.isistan.fakenews.storage.MongoDBStorage
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

import edu.isistan.fakenews.scrapper.*
import edu.isistan.fakenews.storage.Tweet
import edu.isistan.fakenews.storage.UserTweets

import edu.isistan.fakenews.*
import edu.isistan.fakenews.storage.FAKE_NEWS_DB

import java.util.Arrays
import com.mongodb.client.model.Accumulators
import com.mongodb.client.model.Sorts
import com.mongodb.client.model.Aggregates
import twitter4j.Query
import twitter4j.QueryResult
import org.jsoup.Jsoup

import edu.isistan.fakenews.scrapper.*
import java.util.Date
import java.text.SimpleDateFormat
import java.util.Locale
import java.util.GregorianCalendar
import java.time.temporal.ChronoUnit
import java.time.format.DateTimeFormatter
import java.nio.file.StandardOpenOption


class AccessStorage

var LOGGER = LoggerFactory.getLogger(AccessStorage::class.java)!!

fun findScreenshot(storage : MongoDBStorage) {


	storage.tweets.find().forEach{
		println(it.tweetId)
		//			println(it.text)

		if(storage.findScreenshot(it.tweetId) != null){

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

			while (Sequence {
				storage.queries.find().noCursorTimeout(true).iterator()
			}.flatMap { it.tweetIds.asSequence() }.find { storage.findReplies(it) == null } != null) {
				storage.tweets.find(Filters.lt("created", LocalDateTime.now().minusHours(7))).
				noCursorTimeout(true).asSequence().map { it.tweetId }.
				chunked(10000).forEach {
					tweetCrawler.run(it)
				}
			}

	//		storage.tweets.find(Filters.lt("created",LocalDateTime.now().minusHours(7))).asSequence().map{it.tweetId}.chunked(3).forEach{
	//			tweetCrawler.run(it)
	//		}
}

fun formQuery(storage : MongoDBStorage){
	val query = "FilterQuery{count=0, follow=[33989170, 144929758, 149991703, 4515126989, 69416519, 8105922, 54414081, 2953955753, 37494271, 35776604, 171650522, 152325528], track=[quedateencasa,  covid,  covid-19,  casa rosada,  cuarentena,  pami,  barbijo,  mÃƒÂ¡scara,  salud,  coronavirus,  solidaridad,  impuesto,  argentina,  caso,  muert,  infectado,  infectada,  test,  testeo,  testeomasivo,  tapaboca,  mÃƒÂ©dico,  enfermera,  viruschino,  virus,  virus chino,  jubilado,  pandemia,  mayorescuidados,  tapatelaboca,  cuidarteescuidarnos,  desarrollo social,  cancilleria argentina,  argentinaunida,  deuda], locations=null, language=[es], filter_level=null}\n"
			storage.findOrStoreQuery(query,storage.tweets.find().map{it.tweetId}.toMutableList())
}

fun downloadUserCumulative(storage : MongoDBStorage, perc : Long){
	val result = storage.tweets.aggregate(Arrays.asList(
			Aggregates.group("$"+"userId", Accumulators.sum("count", 1)),
			Aggregates.sort(Sorts.descending("count"))), Document::class.java).allowDiskUse(true).filter{it.getLong("_id") > 0}

	val total = result.sumBy { it.getInteger("count") } * perc//el acumulado para los que quiero bajar...

			var sum = 0
			val toDownload = mutableSetOf<Long>()
			toDownload.add(result.first().getLong("_id"))
			result.forEach{
		sum += it.getInteger("count")
				if(sum < total)
					toDownload.add(it.getLong("_id"))
	}

	//TODO: Acá se pueden descargar los usuarios
	println(toDownload)
}

//load ids from file and call userDownload with them
fun downloadUsers(storage : MongoDBStorage, filename : String){
	val file = File(filename)
			val users = ArrayList<Long>()
			file.readLines().map { it.trim() }.filter { it.isNotEmpty() }.forEach {
				val sp = it.split(",")
						if(sp[1].toInt() >= 1000)
							users.add(sp[0].toLong())
			}
			println(users)
			val crawler = TwitterCrawler(storage)

			users.reverse()
			crawler.usersCrawl(users.toLongArray(),false,true,true)
}

//check which users are incomplete and redownload the data
fun checkUsers(storage : MongoDBStorage){

	val users_to_fix = mutableSetOf<Long>()

			storage.users.find(Filters.exists("verified", false) ).iterator().forEach{
		users_to_fix.add(it.userId)
	}

	println(users_to_fix.size)

}

fun TwitterCrawler.search(query : Query) : QueryResult{
	return twitter.search(query)
}

// Given a set of tweets, search tweets from that users written during the covid period and including any of related keywords
fun searchTweetsUsers(filename : String, storage : MongoDBStorage, queryText : String, since: String, until: String){

	val twitter = TwitterCrawler(storage)
			val driver = initFirefoxWithScrapperExtension()

			val file = File(filename)
			file.readLines().map { it.trim() }.filter { it.isNotEmpty() }.
			asSequence().chunked(200).forEach{ // every 200 tweets

				val tweets = mutableListOf<Long>()

						it.mapNotNull{t -> storage.findTweet(t.toLong())?.userId}. // tweets in database
						distinct().  
						filter{u -> !storage.userTweetsPresent(u)}. // users without tweets in database
						forEach{u ->
						val sn = storage.findUser(u)?.screenName
								if(sn != null){
									val ut = getUserTweets(sn,"since: $since until: $until $queryText",driver)
											storage.storeUserTweets(u,ut.toList())
											tweets.addAll(ut)
								}
						}
						twitter.run(tweets)
			}

			driver.quit()
}

fun deleteTweetsUsers(storage : MongoDBStorage, words : List<String>, since : String, until: String){ // users with tweets that do not match the selected keywords can be deleted

	val formatter = SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
	val since_d = formatter.parse(since)
			val until_d = formatter.parse(until)

			storage.userTweets.find().noCursorTimeout(true).forEach{
		val (to_keep, to_remove) = it.tweets.mapNotNull{t -> storage.findTweet(t)}.
				partition{tweet ->
				tweet.created.after(since_d) &&
				tweet.created.before(until_d) &&
				words.any{it in tweet.text.toLowerCase()}}

		storage.tweets.deleteMany(Filters.`in`("tweetId",to_remove.map{t -> t.tweetId}))
		storage.userTweets.replaceOne(Filters.eq("userId",it.userId),UserTweets(null, it.userId, to_keep.map{t -> t.tweetId}.toMutableList()))

	}

}

fun listTweetIdsUsers(storage : MongoDBStorage, filename : String?){ // lists and saves all tweet ids to be used for another download process (replies)

	val users = mutableListOf<Long>()
			if(filename != null){
				val file = File(filename)
						file.readLines().map { it.trim() }
				.filter { it.isNotEmpty() }
				.mapNotNull { storage.findUser(it) }
				.forEach { users.add(it.userId) }
			}

	val writer = Files.newBufferedWriter(File("muni_tweets.txt").toPath());

	storage.tweets.find().noCursorTimeout(true)
	.filter{(it.retweetId == null || it.retweetId!! < 0) && (users.isEmpty() || users.contains(it.userId))}
	.forEach{
		writer.write(it.tweetId.toString())	
		writer.newLine()
	}

	writer.close()
}

fun deleteRepliesTweets(storage : MongoDBStorage, filename : String){

	val file = File(filename)
			val tweets = file.readLines().map { it.trim() }
	.filter { it.isNotEmpty() }.map{it.toLong()}.toList()


	storage.tweetReplies.deleteMany(Filters.`nin`("tweetId",tweets))
}

fun removeRepliesFromUser(storage : MongoDBStorage, username : String){

	val userId = storage.findUser(username)!!.userId

			var tweets_user = storage.tweets.find(Filters.eq("userId",userId)).map{it.tweetId}.toList()

			tweets_user = tweets_user.mapNotNull{storage.findReplies(it)}.filter{it.replies.size == 0}.map{it.tweetId}.toList()

			println(tweets_user)

			storage.tweetReplies.deleteMany(Filters.`in`("tweetId",tweets_user))

}

fun removeAlreadyProcessed(storage : MongoDBStorage, filename : String){
	val file = File(filename)
			val tweets = file.readLines().map { it.trim() }
	.filter { it.isNotEmpty() }.map{it.toLong()}.toSet()

	val writer = Files.newBufferedWriter(File("MISSING_${file.name}").toPath());

	tweets.filter{storage.findReplies(it) == null || storage.findReactions(it,"retweeted") == null}
	.forEach{
		writer.write(it.toString())	
		writer.newLine()
	}

	writer.close()
}

// esto creo que era para los municipios...
//Por cada uno de los <tweetID, [replies]> mirar cuáles no están bajados y pasarlos a una lista
//To solve a problem in which replies were gathereded but not saved - buscar en los de replies los tweets que falten
fun getMissingReplies(storage : MongoDBStorage){ 

	val writer = Files.newBufferedWriter(File("missing_replies.txt").toPath());

	storage.tweetReplies.find().noCursorTimeout(true).iterator().forEach{
		it.replies.filter{storage.findTweet(it) == null}.forEach{
			writer.write(it.toString())	
			writer.newLine()	
		}
	}

	writer.close()

}

//from: XX until: first tweet since: un par de semanas antes while since > created --> tratar de recuperar los tweets más viejos --> parecido al de abajo, solo que las fechas van al revés
fun tweetsFromAccounts(storage:MongoDBStorage, filename : String){
	//		val file = File(filename)
	//	val usernames = file.readLines().map { it.trim() }
	//						.filter { it.isNotEmpty() }.toList()
	//
	//	val now = Instant.now()
	//	
	//	val writer = Files.newBufferedWriter(File("tweets_from_munis.txt").toPath());
	//	
	//	val days = 21L
	//	val reg = Regex("T.*")
	//	
	//	usernames.mapNotNull{storage.findUser(it)}.forEach{
	//		val name = it.screenName
	//		val created_at = it.created.toInstant()
	//		
	//		
	//		var until = storage.userTweets.find(Filters.eq("userId",it.userId)).noCursorTimeout(true)
	//						  .flatMap{it.tweets}.mapNotNull{t -> storage.findTweet(t.toLong())?.created}.min()?.toInstant()
	//			
	////		until = if (until == null) created_at
	//		
	//		var since = until.minus(days,ChronoUnit.DAYS)
	//		
	//		val tweets = mutableSetOf<Long>()
	//		
	//		while (until.isAfter(created_at)){
	//			
	//			val untilP = since.truncatedTo(ChronoUnit.DAYS).toString().replace(reg,"")
	//			val sinceP = since.truncatedTo(ChronoUnit.DAYS).toString().replace(reg,"")
	//			
	//			println("$sinceP  $untilP")
	//			
	//			tweets.addAll(getScrappedSearch("from:$name until:$untilP since:$sinceP"))
	//			
	//			since = until
	//			until = since.minus(days,ChronoUnit.DAYS)
	//		}
	//		
	////		//save tweets in file
	//		tweets.forEach{
	//			writer.write(it.toString())	
	//			writer.newLine()	
	//		}
	//	}
	//	writer.close()
}

//tweets mentioning accounts since the created time of the account
fun tweetsMentioningAccounts(storage : MongoDBStorage, filename : String){

	val file = File(filename)
			val usernames = file.readLines().map { it.trim() }
	.filter { it.isNotEmpty() }.toList()

	val now = Instant.now()

	val writer = Files.newBufferedWriter(File("mentions_to_munis.txt").toPath());

	val days = 21L
			val reg = Regex("T.*")
			usernames.mapNotNull{storage.findUser(it)}.forEach{
				val name = it.screenName
						val created_at = it.created

						var since = created_at.toInstant()
						var until = since.plus(days,ChronoUnit.DAYS)

						val mentions = mutableSetOf<Long>()

						while (since.isBefore(now)){

							val untilP = until.truncatedTo(ChronoUnit.DAYS).toString().replace(reg,"")
							val sinceP = since.truncatedTo(ChronoUnit.DAYS).toString().replace(reg,"")

							println("$sinceP  $untilP")

							mentions.addAll(getScrappedSearch("@$name since:$sinceP until:$untilP"))
							println("mentions $mentions")
							since = until
							until = since.plus(days,ChronoUnit.DAYS)
						}

				//		//save tweets in file
				mentions.forEach{ // esto es lo que hay que bajar después
					writer.write(it.toString())	
					writer.newLine()	
				}
				writer.flush();
			}
			writer.close()
}

//quotes de los tweets de las cuentas en el archivo para la búsqueda es: url:TWEET_ID usando el search
fun searchQuotes(storage : MongoDBStorage?, filename : String){

	val file = File(filename)
	val tweets = file.readLines().map { it.trim() }
								 .filter { it.isNotEmpty() }.toSet()

//	println(tweets)
	val writer = Files.newBufferedWriter(File("quotes_to_munis.txt").toPath(),StandardOpenOption.CREATE, StandardOpenOption.APPEND);

	tweets//.mapNotNull{storage.findTweet(it.toLong())}
		.asSequence().chunked(7)
		.forEach{ tweetIds ->
//			println(tweetIds)
			
			val	tt = StringBuilder()
			tweetIds.forEach{tt.append(" OR url:${it}")}
			
//			println(tt.substring(3))
			
//			val quotes = getScrappedSearch("url:${tweetIds}")
			val quotes = getScrappedSearch(tt.substring(3))
//			
			tweetIds.forEach{quotes.remove(it.toLong())}
			println("quotes: $quotes")
						
			quotes.forEach{ 
				writer.write(it.toString())	
				writer.newLine()	
			}
		writer.flush()
	}
	writer.close()

}

