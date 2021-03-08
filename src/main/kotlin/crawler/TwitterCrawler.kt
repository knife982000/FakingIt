package edu.isistan.fakenews.crawler

import edu.isistan.fakenews.storage.MongoDBStorage
import edu.isistan.fakenews.storage.USER_FOLLOWEES_COLLECTION
import edu.isistan.fakenews.storage.USER_FOLLOWERS_COLLECTION
import org.slf4j.LoggerFactory
import twitter4j.*
import twitter4j.HttpResponseCode.UNAUTHORIZED
import twitter4j.HttpResponseCode.FORBIDDEN
import twitter4j.conf.Configuration
import twitter4j.conf.ConfigurationBuilder
import java.io.File
import java.lang.Exception
import java.lang.Thread.sleep
import java.util.*
import kotlin.system.exitProcess

import edu.isistan.fakenews.storage.Tweet
import org.bson.types.ObjectId
import java.time.ZoneId
import edu.isistan.fakenews.scrapper.*
import org.openqa.selenium.WebDriver
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import com.mongodb.client.model.Filters

var DEBUG_CRAWLER = false
val LOGGER = LoggerFactory.getLogger(TwitterCrawler::class.java)

data class TwitterAuth(var oAuthConsumerKey: String,
		var OAuthConsumerSecret: String,
		var oAuthAccessToken: String,
		var oAuthAccessTokenSecret: String,
		var restBaseURL: String,
		var confName: String="")

fun Twitter.lookupUsers404(vararg ids: Long): List<User>{
	try {
		return this.lookupUsers(*ids)
	} catch (e: TwitterException) {

		if (e.statusCode == HttpResponseCode.NOT_FOUND) {
			LOGGER.warn("Users not found ${longArrayOf(*ids).contentToString()}")
			return emptyList<User>()
		}
		throw e
	}
}

fun Twitter.getFriendsList404(id: Long, cursor : Long): PagableResponseList<User>?{
	try {
		return this.getFriendsList(id,cursor)
	} catch (e: TwitterException) {

		if (e.statusCode == HttpResponseCode.NOT_FOUND) {
			LOGGER.warn("User friends not found ${id}")
			return null //TODO: Change to avoid returning null
		}
		throw e
	}
}

fun Twitter.getFollowersList404(id: Long, cursor : Long): PagableResponseList<User>?{
	try {
		return this.getFriendsList(id,cursor)
	} catch (e: TwitterException) {

		if (e.statusCode == HttpResponseCode.NOT_FOUND) {
			LOGGER.warn("User followers not found ${id}")
			return null //TODO: Change to avoid returning null
		}
		throw e
	}
}

fun Twitter.getRetweets404(id: Long): List<Status>{
	try {
		return this.getRetweets(id)
	} catch (e: TwitterException) {

		if (e.statusCode == HttpResponseCode.NOT_FOUND) {
			LOGGER.warn("Retweets not found ${id}")
			return mutableListOf<Status>()
		}
		throw e
	}
}

fun TwitterAuth.createConfiguration(streamAPI : Boolean = false): Configuration {

	val configurationToken = ConfigurationBuilder().
			setOAuthConsumerKey(this.oAuthConsumerKey).
			setOAuthConsumerSecret(this.OAuthConsumerSecret).
			setApplicationOnlyAuthEnabled(true)
			if(!streamAPI)
				try {
					val token = TwitterFactory(configurationToken.build()).getInstance().getOAuth2Token()

							val configurationBuilder = ConfigurationBuilder().
							setApplicationOnlyAuthEnabled(true).
							setOAuthConsumerKey(this.oAuthConsumerKey).
							setOAuthConsumerSecret(this.OAuthConsumerSecret).
							setOAuth2TokenType(token.getTokenType()).
							setOAuth2AccessToken(token.getAccessToken()).
							//        setOAuthAccessToken(this.oAuthAccessToken).	
							//        setOAuthAccessTokenSecret(oAuthAccessTokenSecret).
							setRestBaseURL(this.restBaseURL).
							setDebugEnabled(DEBUG_CRAWLER)

							return configurationBuilder.build()
				} catch(e:Exception) {

				}
	val configurationBuilder = ConfigurationBuilder(). //TODO
			setOAuthConsumerKey(this.oAuthConsumerKey).
			setOAuthConsumerSecret(this.OAuthConsumerSecret).
			setOAuthAccessToken(this.oAuthAccessToken).	
			setOAuthAccessTokenSecret(oAuthAccessTokenSecret).
			setRestBaseURL(this.restBaseURL).
			setDebugEnabled(DEBUG_CRAWLER)

			return configurationBuilder.build()
}

fun loadConfigurations() : Set<ConfigurationActivation> {

	val configurations = sortedSetOf<ConfigurationActivation>()

			val directory = System.getProperty("twitter.oauthdir", "oauth")
			val dir = File(directory)
			if (!dir.exists()){
				LOGGER.error("Directory $directory not found! Cannot load twitter oauth.")
				exitProcess(1)
			}
	if (!dir.isDirectory){
		LOGGER.error("$directory is not a directory! Cannot load twitter oauth.")
		exitProcess(1)
	}
	dir.listFiles()!!.filter { it.name.endsWith(".properties") }
	.map {
		LOGGER.debug("Loading: {}", it.name)
		val properties = Properties()
		properties.load(it.inputStream())
		var result: TwitterAuth? = null
		try {
			result = TwitterAuth(properties.getProperty("oauth.consumerKey")!!,
					properties.getProperty("oauth.consumerSecret")!!,
					properties.getProperty("oauth.accessToken")!!,
					properties.getProperty("oauth.accessTokenSecret")!!,
					properties.getProperty("restBaseURL")!!,
					it.name)
		} catch (e: KotlinNullPointerException){

		}
		result
	}.filter {
		it != null
	}.forEach{
		configurations.add(ConfigurationActivation(System.currentTimeMillis()-1, it!!))
	}
	return configurations
} 


data class ConfigurationActivation(val time: Long, val configuration: TwitterAuth) :
	Comparable<ConfigurationActivation> {
	override fun compareTo(other: ConfigurationActivation): Int = when {
		this.time < other.time -> -1
		this.time > other.time -> 1
		else -> this.configuration.oAuthAccessToken.compareTo(other.configuration.oAuthAccessToken)
	}

}

class TwitterCrawler(val storage: MongoDBStorage): AutoCloseable {

	var LOGGER = LoggerFactory.getLogger(TwitterCrawler::class.java)!!

			private var driver: WebDriver? = null

			private val configurations = sortedSetOf<ConfigurationActivation>()
			private lateinit var currentConfiguration: TwitterAuth

			lateinit var twitter: Twitter

			init {
		LOGGER.info("Initializing twitter crawler")
		configurations.addAll(loadConfigurations())
		this.nextTwitterConnection(null)
	}

	private fun nextTwitterConnection(timeout: Long?) {
		if (timeout != null) {
			this.configurations.add(ConfigurationActivation(timeout, currentConfiguration))
		}
		if (this.configurations.isEmpty()) {
			LOGGER.error("There are no aouth configured!. Exiting program.")
			exitProcess(1)
		}
		val next = this.configurations.pollFirst()!!
				LOGGER.info("Initializing Twitter account")
				val current = System.currentTimeMillis()
				if (next.time > current) {
					val wait = next.time - current + 1000
							LOGGER.info("Next configuration is not ready... waiting {}", wait)
							sleep(wait)
							LOGGER.info("Continuing")
				}
		currentConfiguration = next.configuration
				LOGGER.info("Setting {} Twitter configuration", currentConfiguration.confName)
				twitter = TwitterFactory(currentConfiguration.createConfiguration()).instance
	}

	fun run(recursive : Boolean = false) {
		this.retryTwitterDownloadWrapper { this.twitterCrawl() }

		run(storage.findTweets().toMutableList(),recursive) //it is gonna skip the first line as all tweets already exist!

	}

	//in case tweets are downloaded, we need to check the state of users, if user == -1 need to re download tweet
	fun run(tweetIds : List<Long>,recursive : Boolean=false,downloadReplies : Boolean=false) {

		this.retryTwitterDownloadWrapper { this.twitterCrawl(tweetIds) }

//		val tweetIdsFiltered = tweetIds.filter{this.storage.findTweet(it) != null}
		
//		val mistaken = retrieveMistakenTweets(tweetIdsFiltered.filter{this.storage.findTweet(it)!!.userId < 0}.toList())
//		
//		mistaken.forEach{
//			if(it.user != null && it.user!!.id > 0)
//				this.storage.updateTweet(it)
//		}
		
		var tweetIdsFiltered = mutableListOf<Long>()
		
		 tweetIds.asSequence().filter{
			val tweet = this.storage.findTweet(it)
			tweet != null && tweet.userId < 0}
			 				.chunked(99)
			 				.forEach{
			 					val mistaken = retrieveMistakenTweets(it)
			 					mistaken.forEach{
			 					if(it.user != null && it.user!!.id > 0)
			 						this.storage.updateTweet(it)
			 					}
			 					tweetIdsFiltered.addAll(it)
			 				}
		
		tweetIdsFiltered.mapNotNull { this.storage.findTweet(it) }
						.map{it.userId}
						.filter{l -> l > 0 && this.storage.findUser(l) == null}
						.chunked(1000)
						.forEach{usersCrawl(it.toLongArray(),false,false,false)}
							
		if(downloadReplies){
			
				tweetIdsFiltered = tweetIds.toMutableList()
				
		tweetIdsFiltered.asSequence().filter{this.storage.findTweet(it) != null && this.storage.findTweet(it)!!.userId > 0}
						.chunked(99)
						.forEach{this.tweetReplyDownload(it,recursive)}
					
//			this.tweetReactionsDownload(tweetIdsFiltered2,"favorited") //scrapper
//			this.tweetReactionsDownload(tweetIdsFiltered2,"retweeted") //scrapper

//			this.tweetReactionsDownload(tweetIds,"favorited") //scrapper
//			this.tweetReactionsDownload(tweetIds,"retweeted") //scrapper
		
//			this.tweetReplyDownload(tweetIdsFiltered2,recursive) //scrapper

			this.tweetRetweetDownload(tweetIdsFiltered)
			
		}

			//this.usersCrawlToDownload()
	}

	fun retrieveMistakenTweets(mistaken : List<Long>) : MutableList<Status> {
		val retrieved = mutableListOf<Status>()

				mistaken.
				chunked(99).map {
			it.toLongArray()
		}.forEach {
			retryTwitterDownloadWrapper {
				LOGGER.info("Downloading tweets {}", it)
				twitter.lookup(*it).forEach{retrieved.add(it)}
			}
		}

		return retrieved
	}

	/**
	 * Downloads the parents of the tweets in the list id
	 * @param tweetIds tweet ids to download. They must be already downloaded
	 * @param downloadFull selects whether to download only tweets or their replies and reactions
	 * @return The map of the tweetid to tree root tweetId
	 * **/
	fun downloadInReplyToTweets(tweetIds: List<Long>, downloadFull: Boolean=false, recursive : Boolean=false): Map<Long, Long> {
		val finalMap = tweetIds.map { it to it }.toMap().toMutableMap()
				var download = tweetIds
				LOGGER.info("Downloading repliesreplies")
				while (download.isNotEmpty()) {
					LOGGER.debug("Downloading replies: {}", download.size)
					val local = download.map { this.storage.findTweet(it) }.
					filterNotNull().
					filter { it.inReplyToStatusId != -1L }.
					map { it.tweetId to it.inReplyToStatusId }.toMap()
					finalMap.replaceAll { _, v -> local[v]?: v }
					download = local.values.toMutableList()
							if (downloadFull) {
								this.run(download,recursive)
							} else {
								this.twitterCrawl(download)
							}

				}
		LOGGER.info("No more replies found")
		return finalMap
	}

	private fun retryTwitterDownloadWrapper(action: ()->Unit) {
		assert(this::twitter.isInitialized) { "TwitterCrawler was not initialized" }
		var completed = false
				var networkFails = 0
				while (!completed) {
					completed = try {
						action()
						networkFails = 0
						true
					} catch (e: TwitterException) {
						LOGGER.warn("Attempting to recovery from an error. Number of network fails {}. Error: {}", networkFails, e.errorMessage)
						when {
							e.exceededRateLimitation() || (e.errorMessage != null && e.errorMessage.contains("orbidden")) -> {
								LOGGER.warn(e.rateLimitStatus.toString())
								val retryIn = e.rateLimitStatus.resetTimeInSeconds.toLong() * 1000
								nextTwitterConnection(retryIn)
							}
							e.errorCode == UNAUTHORIZED -> { //Forbidden is not the same, but that's the error we get, and the time is not
								LOGGER.warn("Account $currentConfiguration was unautorized. Removing it from valid configurations.")
								nextTwitterConnection(null)
							}
							e.isCausedByNetworkIssue -> {
								networkFails += 1
										sleep((1000 * networkFails).toLong())
							}
							
							else -> {
								LOGGER.error("Unexpected error in twitter ", e)
								nextTwitterConnection(100_000)
							}
						}
						false //A Re-try is needed
					} catch (e: Exception) {
						LOGGER.error("Unexpected Error. Finishing crawler", e)
						exitProcess(1)
					}
				}
	}

	fun usersCrawl(ids: LongArray, tweets: Boolean=true, followers: Boolean=true, followees: Boolean=true) {
		LOGGER.info("Downloading user list")
		ids.filter {
			this.storage.findUser(it) == null
		}.chunked(99).map {
			it.toLongArray()
		}.forEach {
			retryTwitterDownloadWrapper {
				twitter.lookupUsers404(*it).forEach {
					this.storage.storeUser(it)
				}
			}
		}
		ids.filter{this.storage.findUser(it) != null}.forEach { userCrawl(it, tweets, followers, followees) }
	}

	fun repairUsers(){
		
		Sequence { storage.users.find(Filters.exists("verified", false)).noCursorTimeout(true).iterator() }.
				map{it.userId}.chunked(99).map {
			it.toLongArray()
		}.forEach {
			retryTwitterDownloadWrapper {
				twitter.lookupUsers404(*it).forEach {
					this.storage.replaceUser(it)
				}
			}
		}
	}
	
	fun usersCrawl(ids: MutableSet<String>) {
		LOGGER.info("Downloading user list {}",ids)
		ids.filter {
			this.storage.findUser(it) == null
		}.chunked(99).map {
			it.toTypedArray()
		}.forEach {
			retryTwitterDownloadWrapper {
				twitter.lookupUsers(*it).forEach {
					this.storage.storeUser(it)
				}
			}
		}
		//			ids.forEach { userCrawl(it, tweets, followers, followees) }
	}

	fun userCrawl(userId: Long, tweets: Boolean=true, followers: Boolean=true, followees: Boolean=true, replies: Boolean=true) {
		LOGGER.info("Downloading User {}", userId)
		//		if (this.storage.findUser(userId) == null) {
		//			LOGGER.debug("User {} is not present. Downloading information.", userId)
		//			retryTwitterDownloadWrapper {
		//				twitter.lookupUsers404(userId).forEach {
		//					this.storage.storeUser(it)
		//				}
		//			}
		//		}
		if (tweets && !this.storage.userTweetsPresent(userId)) {
			LOGGER.info("Downloading tweets for User {}", userId)
			retryTwitterDownloadWrapper {
				LOGGER.debug("Downloading pages...")
				@Suppress("NAME_SHADOWING") val tweets = mutableListOf<Status>()
				val page = Paging(1)
				while (true) {
					LOGGER.debug("Page: {} UserId: {}", page, userId)
					val pageTweets = twitter.getUserTimeline(userId, page)
					tweets.addAll(pageTweets)
					if (pageTweets.isEmpty())
						break
						page.page += 1
				}
				tweets.forEach { this.storeTweet(it) } 
				this.storage.storeUserTweets(userId, tweets.map { it.id })
				if(replies)
					tweets.map{t -> t.id}.chunked(100).forEach{tweetReplyDownload(it)}	
			}
		}
		if (followees && !this.storage.userFolloweesFulledDownload(userId)) {
			LOGGER.info("Downloading followees for User {}", userId)
			var cursor = this.storage.findDownloadCursor(userId, USER_FOLLOWEES_COLLECTION)?: -1L
					retryTwitterDownloadWrapper {
				LOGGER.debug("Downloading...")
				while (true) {
					LOGGER.debug("Cursor followees: {} UserId: {}", cursor, userId)
					val cursorFollowees = twitter.getFriendsList404(userId, cursor)
					if(cursorFollowees == null)
						break;
					this.storage.storeUserFollowees(userId, cursorFollowees)
					if (!cursorFollowees.hasNext())
						break
						cursor = cursorFollowees.nextCursor
						this.storage.storeDownloadCursor(userId, cursor, USER_FOLLOWEES_COLLECTION)
				}
				this.storage.removeDownloadCursor(userId, USER_FOLLOWEES_COLLECTION)
			}
		}
		if (followers && !this.storage.userFollowersFulledDownload(userId)) {
			LOGGER.info("Downloading followers for User {}", userId)
			var cursor = this.storage.findDownloadCursor(userId, USER_FOLLOWERS_COLLECTION)?: -1L
					retryTwitterDownloadWrapper {
				LOGGER.debug("Downloading followers...")
				while (true) {
					LOGGER.debug("Cursor followers: {} UserId: {}", cursor, userId)
					val cursorFollowers = twitter.getFollowersList404(userId, cursor)
					if(cursorFollowers == null)
						break;
					this.storage.storeUserFollowers(userId, cursorFollowers)
					if (!cursorFollowers.hasNext())
						break
						cursor = cursorFollowers.nextCursor
						this.storage.storeDownloadCursor(userId, cursor, USER_FOLLOWERS_COLLECTION)
				}
				this.storage.removeDownloadCursor(userId, USER_FOLLOWERS_COLLECTION)
			}
		}
		LOGGER.info("User {} downloaded", userId)
	}

	private fun usersCrawlToDownload() {
		while (true) {
			val usersIdDownload = this.storage.nextBatchUserDownload().map {
				it.userId
			}.toLongArray()
					if (usersIdDownload.isEmpty()) {
						break
					}
			LOGGER.info("Downloading batch of users. Size: {}", usersIdDownload.size)
			this.usersCrawl(usersIdDownload)
			usersIdDownload.forEach {
				this.storage.removeUserDownload(it)
			}
		}
		LOGGER.info("All users have been downloaded")
	}

	//added extra parameters for all queries
	private fun twitterCrawl(){ 
		val currentQuery = this.storage.nextQueryDownload()
				if (currentQuery == null) {
					LOGGER.info("There are no more queries to crawl...")
					return
				}
		var queryInfo = this.storage.findOrStoreQuery(currentQuery.text)
				LOGGER.info("Processing query {}", currentQuery)
				var query = Query(currentQuery.text)
				query.maxId = currentQuery.maxId

				//new parameters
				val until = System.getProperty("search.until",null)
				val since = System.getProperty("search.since",null)
				val language = System.getProperty("search.language",null)
				val location = System.getProperty("search.location",null)
				val resultType = System.getProperty("search.resultType",null)	

				if(until != null) query.setUntil(until)
				if(since != null) query.setSince(since)
				if(language != null) query.setLang(language)
				if(location != null){
					val loc = location.split(",")
							if(loc.size >= 2)
								query.geoCode(GeoLocation(loc[0].toDouble(),loc[1].toDouble()),25.0,Query.KILOMETERS)
								else
									LOGGER.warn("Wrong number of coordinates at search.location")
				}

		if(resultType != null)
			when (resultType.toLowerCase()) {
			"popular" -> query.setResultType(Query.POPULAR)
			"recent"-> query.setResultType(Query.RECENT)
		}

		while (true) {
			LOGGER.info("Processing batch...")
			val results = twitter.search(query)
			results.tweets.forEach{ this.storeTweet(it) }
			queryInfo.tweetIds.addAll(results.tweets.map { it.id })
			//Store relation between query and tweet
			queryInfo = this.storage.findOrStoreQuery(queryInfo)
			if (results.hasNext()) {
				query = results.nextQuery()
						currentQuery.maxId = query.maxId
						this.storage.findOrStoreQueryDownload(currentQuery)
			} else {
				this.storage.removeQueryDownload(currentQuery)
				return
			}
		}
	}

	//for each tweet --> get replies, add all replies to tweetReplies, add replies ids to list
	//call twitterCrawl al final para bajar los tweets de las respuestas.
	//call tweetReplyDownload para armar la cadena de las cadenas
	private fun tweetReplyDownload(tweetIds : List<Long>,recursive : Boolean = false){
		if (this.driver==null) {
			this.driver = initFirefoxWithScrapperExtension()
		}
		val newIds = ArrayList<Long>()
		tweetIds.filter{this.storage.findReplies(it) == null}.forEach{
			val tweet = this.storage.findTweet(it)!!
			val username = this.storage.findUser(tweet.userId)!!.screenName //there is no need to have it as it can be replaced by any

			var replies = getScrapedReplies(username,tweet.tweetId.toString())
			
			if(replies.remove(-1L)) // if there was an error or there were more replies than those we could get
				replies = getReplies(username,tweet.tweetId.toString(), driver)
													
				this.storage.storeTweetReplies(it,replies)
				newIds.addAll(replies)
			
				if(newIds.size > 99){ //for every 100 tweets we found, we download them
					retryTwitterDownloadWrapper { twitterCrawl(newIds) }
					if(recursive) tweetReplyDownload(newIds)
					newIds.clear()
				}
			
		}

		if (newIds.isEmpty())
			return

		retryTwitterDownloadWrapper { twitterCrawl(newIds) }
		if(recursive) tweetReplyDownload(newIds)
	}

	private fun twitterCrawl(tweetIds : List<Long>){

		tweetIds.asSequence().filter{this.storage.findTweet(it) == null}. // sequence {tweetsIds.filter }
		chunked(99).map {
			it.toLongArray()
		}.forEach {
			retryTwitterDownloadWrapper {
				LOGGER.info("Downloading tweets {}", it)
				twitter.lookup(*it).forEach{
					this.storage.storeTweet(it)
				}
			}
		}		
	}

	private fun tweetReactionsDownload(tweetIds : List<Long>, what : String, threads: Int=70){
//		val sem = Semaphore(threads);
//		val exec = Executors.newCachedThreadPool()
//				tweetIds.filter{!this.storage.isReactionsStored(it,what)}.forEach{
//					sem.acquire()
//					exec.submit {
//						try {
//							val reactions = getReactions(it.toString(), what)
//									this.storage.storeTweetReactions(it, reactions, what) //here we could add all users to usersDownload
//						} finally {
//							sem.release()
//						}
//					}
//				sem.acquire(threads)
//				exec.shutdown()
	}

	fun tweetRetweetDownload(tweetIds : List<Long>){
		
		tweetIds.asSequence().filter{this.storage.findTweet(it) != null && this.storage.findReactions(it,"retweeted") == null}.
		forEach {
			
			if(getRetweetCount(it.toString()) == 0L){
				this.storage.storeTweetReactions(it,emptyList(),"retweeted")
			}
			else{
				retryTwitterDownloadWrapper {
				val retweeters = mutableListOf<Long>()
				LOGGER.info("Downloading retweets {}", it)
				twitter.getRetweets404(it).forEach{
					this.storage.storeTweet(it)
					retweeters.add(it.getUser().id)
				}
				LOGGER.info("Storing retweeters {}", it)
				this.storage.storeTweetReactions(it,retweeters,"retweeted")
			
			}
		}
		}
	}
	
	fun getReplies(username: String, tweetId: Long, recursive: Boolean=true, maxDays: Long = 4): Map<Long, List<Tweet<ObjectId>>> {
		val result = mutableMapOf<Long, List<Tweet<ObjectId>>>()
				var baseTweet = storage.findTweet(tweetId)
				if (baseTweet == null) {
					this.twitterCrawl(listOf(tweetId))
					baseTweet = storage.findTweet(tweetId)
					if (baseTweet == null) {
						LOGGER.error("Tweet unavailable.")
						return emptyMap()
					}
				}
		val date = baseTweet.created.toInstant().atZone(ZoneId.systemDefault()).toLocalDate()
				val since = date.minusDays(1).toString()
				val until = date.plusDays(maxDays).toString()

				var candidates = getSearchedPotentialReplies(username, tweetId, since, until)
				val replies = mutableListOf<Tweet<ObjectId>>()
				//Como los tengo que bajar de todas maneras, los bajo y guardo
				this.twitterCrawl(candidates)
				//Proceso los tweets que ya tengo
				candidates.forEach {
			val tweet = storage.findTweet(it)
					if (tweet!=null) {
						if (tweet.inReplyToStatusId == tweetId) {
							replies.add(tweet)
						}
					} else {
						LOGGER.warn("Missing tweet: {}", it)
					}
		}

		result[tweetId] = replies
				if (recursive) {
					replies.map {
						it to storage.findUser(it.userId)
					}.filter {
						it.second != null
					}.forEach {
						result.putAll(this.getReplies(it.second!!.screenName, it.first.tweetId))
					}
				}
		return result
	}


	private fun storeTweet(tweet: Status){
		LOGGER.debug("Processing Storage tweet: {}", tweet.id)
		if (this.storage.storeTweet(tweet)) {
			val urls = mutableSetOf<String>()
					urls.addAll(tweet.urlEntities.map { it.expandedURL })
					urls.addAll(tweet.mediaEntities.map { it.expandedURL })
					if (tweet.user != null) {
						this.storage.storeUserDownload(tweet.user.id)
					}
			urls.forEach { this.storage.storeUrlDownload(it, tweet.id) }
		}
	}

	override fun close() {
		if (this.driver != null) {
			LOGGER.info("Closing Web Driver")
			(this.driver as WebDriver).quit()
				this.driver = null
		}
	
	}

	protected fun finalize() {
		this.close()
	}

}
