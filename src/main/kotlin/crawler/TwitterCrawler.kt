package edu.isistan.fakenews.crawler

import edu.isistan.fakenews.storage.MongoDBStorage
import edu.isistan.fakenews.storage.USER_FOLLOWEES_COLLECTION
import edu.isistan.fakenews.storage.USER_FOLLOWERS_COLLECTION
import org.slf4j.LoggerFactory
import twitter4j.*
import twitter4j.HttpResponseCode.UNAUTHORIZED
import twitter4j.conf.Configuration
import twitter4j.conf.ConfigurationBuilder
import java.io.File
import java.lang.Exception
import java.lang.Thread.sleep
import java.util.*
import kotlin.system.exitProcess

import edu.isistan.fakenews.scrapper.*
import org.openqa.selenium.WebDriver

var DEBUG_CRAWLER = false
var LOGGER = LoggerFactory.getLogger(TwitterCrawler::class.java)!!


data class TwitterAuth(var oAuthConsumerKey: String,
		var OAuthConsumerSecret: String,
		var oAuthAccessToken: String,
		var oAuthAccessTokenSecret: String,
		var restBaseURL: String,
		var confName: String="")

fun TwitterAuth.createConfiguration(): Configuration {

	val configurationToken = ConfigurationBuilder().
			setOAuthConsumerKey(this.oAuthConsumerKey).
			setOAuthConsumerSecret(this.OAuthConsumerSecret).
			setApplicationOnlyAuthEnabled(true)

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


private data class ConfigurationActivation(val time: Long, val configuration: TwitterAuth) :
	Comparable<ConfigurationActivation> {
	override fun compareTo(other: ConfigurationActivation): Int = when {
		this.time < other.time -> -1
		this.time > other.time -> 1
		else -> this.configuration.oAuthAccessToken.compareTo(other.configuration.oAuthAccessToken)
	}

}


class TwitterCrawler(val storage: MongoDBStorage): AutoCloseable {
	private var driver: WebDriver? = null

	private val configurations = sortedSetOf<ConfigurationActivation>()
			private lateinit var currentConfiguration: TwitterAuth

			lateinit var twitter: Twitter

			init {
		LOGGER.info("Initializing twitter crawler")
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

	fun run() {
		this.retryTwitterDownloadWrapper { this.twitterCrawl() }
		this.usersCrawlToDownload()
	}

	fun run(tweetIds : MutableList<Long>) {
		this.retryTwitterDownloadWrapper { this.twitterCrawl(tweetIds) }

		this.tweetReplyDownload(tweetIds) //scrapper
		this.tweetReactionsDownload(tweetIds,"favorited") //scrapper
		this.tweetReactionsDownload(tweetIds,"retweeted") //scrapper

//		this.usersCrawlToDownload()

	}

	/**
	 * Downloads the parents of the tweets in the list id
	 * @param tweetIds tweet ids to download. They must be already downloaded
	 * @param downloadFull selects whether to download only de tweets or thier replies and reactions
	 * @return The map of the tweetid to tree root tweetId
	 * **/
	fun downloadInReplieToTweets(tweetIds: List<Long>, downloadFull: Boolean=false): Map<Long, Long> {
		val finalMap = tweetIds.map { it to it }.toMap().toMutableMap()
		var download = tweetIds
		LOGGER.info("Downloading replies")
		while (download.isNotEmpty()) {
			LOGGER.debug("Downloading replies: {}", download.size)
			val local = download.map { this.storage.findTweet(it) }.
				filterNotNull().
				filter { it.inReplyToStatusId != -1L }.
				map { it.tweetId to it.inReplyToStatusId }.toMap()
			finalMap.replaceAll { k, v -> local[v]?: v }
			download = local.values.toMutableList()
			if (downloadFull) {
				this.run(download)
			} else {
				this.twitterCrawl(download)
			}

		}
		LOGGER.info("No more replies found")
		return finalMap
	}

	private fun retryTwitterDownloadWrapper(action: ()->Unit) {
		assert(this::twitter.isInitialized) { "TwitterCrawler was not initializes" }
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
							e.exceededRateLimitation() -> {
								LOGGER.warn(e.rateLimitStatus.toString())
								val retryIn = e.rateLimitStatus.resetTimeInSeconds.toLong() * 1000
								nextTwitterConnection(retryIn)
							}
							e.errorCode == UNAUTHORIZED -> {
								LOGGER.warn("Account $currentConfiguration was unautorized. Removing it from valid configurations.")
								nextTwitterConnection(null)
							}
							e.isCausedByNetworkIssue -> {
								networkFails += 1
										sleep((1000 * networkFails).toLong())
							}
							else -> {
								LOGGER.error("Unextpected error in twitter ", e)
								nextTwitterConnection(null)
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
				twitter.lookupUsers(*it).forEach {
					System.err.println(it)
					this.storage.storeUser(it)
				}
			}
		}
		ids.forEach { userCrawl(it, tweets, followers, followees) }
	}

	fun userCrawl(userId: Long, tweets: Boolean=true, followers: Boolean=true, followees: Boolean=true) {
		LOGGER.info("Downloading User {}", userId)
		if (this.storage.findUser(userId) == null) {
			LOGGER.debug("User {} is not present. Downloading information.", userId)
			retryTwitterDownloadWrapper {
				twitter.lookupUsers(userId).forEach {
					this.storage.storeUser(it)
				}
			}
		}
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
			}
		}
		if (followees && !this.storage.userFolloweesFulledDownload(userId)) {
			LOGGER.info("Downloading followees for User {}", userId)
			var cursor = this.storage.findDownloadCursor(userId, USER_FOLLOWEES_COLLECTION)?: -1L
					retryTwitterDownloadWrapper {
				LOGGER.debug("Downloading...")
				while (true) {
					LOGGER.debug("Cursor followees: {} UserId: {}", cursor, userId)
					val cursorFollowees = twitter.getFriendsList(userId, cursor)
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
					val cursorFollowers = twitter.getFollowersList(userId, cursor)
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
	//después ver cómo reconstruir !!
	private fun tweetReplyDownload(tweetIds : MutableList<Long>){
		if (this.driver==null) {
			this.driver = initFirefoxWithScrapperExtension()
		}
		val newIds = ArrayList<Long>()
		tweetIds.filter{this.storage.findReplies(it) == null}.forEach{
			val tweet = this.storage.findTweet(it)!!
			val username = this.storage.findUser(tweet.userId!!)!!.screenName
			val replies = getReplies(username,tweet.tweetId.toString(), driver)
						
			this.storage.storeTweetReplies(it,replies)
			newIds.addAll(replies)
		}
		if (newIds.isEmpty())
			return
		retryTwitterDownloadWrapper { twitterCrawl(newIds) }
		tweetReplyDownload(newIds)
	}

	private fun twitterCrawl(tweetIds : MutableList<Long>){

		tweetIds.filter{this.storage.findTweet(it) == null}.
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

	private fun tweetReactionsDownload(tweetIds : List<Long>, what : String){

		for(tweetid in tweetIds){
			if (this.storage.findReplies(tweetid) == null){
				this.storage.storeTweetReactions(tweetid,getReactions(tweetid.toString(),what),what) //here we could add all users to usersDownload
			}

		}
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
			this.driver!!.quit()
		}
	}

	protected fun finalize() {
		this.close()
	}
}