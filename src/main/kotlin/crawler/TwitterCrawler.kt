package edu.isistan.fakenews.crawler
import edu.isistan.fakenews.storage.MongoDBStorage
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



var DEBUG_CRAWLER = false
var LOGGER = LoggerFactory.getLogger(TwitterCrawler::class.java)!!


data class TwitterAuth(var oAuthConsumerKey: String,
                       var OAuthConsumerSecret: String,
                       var oAuthAccessToken: String,
                       var oAuthAccessTokenSecret: String,
                       var restBaseURL: String,
                       var confName: String="")

fun TwitterAuth.createConfiguration(): Configuration {
    val configurationBuilder = ConfigurationBuilder().
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


class TwitterCrawler(val storage: MongoDBStorage) {
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
                TwitterAuth(properties.getProperty("oauth.consumerKey")!!,
                    properties.getProperty("oauth.consumerSecret")!!,
                    properties.getProperty("oauth.accessToken")!!,
                    properties.getProperty("oauth.accessTokenSecret")!!,
                    properties.getProperty("restBaseURL")!!,
                    it.name)
            }.forEach{
                configurations.add(ConfigurationActivation(System.currentTimeMillis()-1, it))
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
        this.usersCrawl()
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


    private fun usersCrawl() {
        while (true) {
            val userDownload = this.storage.nextUserDownload() ?: break
            val tweets = mutableListOf<Status>()
            val followees = mutableListOf<User>()
            val followers = mutableListOf<User>()
            LOGGER.info("Downloading User ${userDownload.userId}")
            LOGGER.info("Downloading tweets for User ${userDownload.userId}")
            val page = Paging(1)
            retryTwitterDownloadWrapper {
                LOGGER.debug("Downloading pages...")
                while (true) {
                    LOGGER.debug("Page: {} UserId: {}", page, userDownload.userId)
                    val pageTweets = twitter.getUserTimeline(userDownload.userId, page)
                    tweets.addAll(pageTweets)
                    if(pageTweets.isEmpty())
                        break
                    page.page += 1
                }
            }
            LOGGER.info("Downloading followees for User ${userDownload.userId}")
            var cursor = -1L
            retryTwitterDownloadWrapper {
                LOGGER.debug("Downloading...")
                while (true){
                    LOGGER.debug("Cursor followees: {} UserId: {}", cursor, userDownload.userId)
                    val cursorFollowees = twitter.getFriendsList(userDownload.userId, cursor)
                    followees.addAll(cursorFollowees)
                    if (!cursorFollowees.hasNext())
                        break
                    cursor = cursorFollowees.nextCursor
                }
            }
            LOGGER.info("Downloading followers for User ${userDownload.userId}")
            cursor = -1L
            retryTwitterDownloadWrapper {
                LOGGER.debug("Downloading followers...")
                while (true) {
                    LOGGER.debug("Cursor followers: {} UserId: {}", cursor, userDownload.userId)
                    val cursorFollowers = twitter.getFollowersList(userDownload.userId, cursor)
                    followers.addAll(cursorFollowers)
                    if (!cursorFollowers.hasNext())
                        break
                    cursor = cursorFollowers.nextCursor
                }
            }
            LOGGER.info("Storing information for User ${userDownload.userId}")
            followees.forEach { this.storage.storeUser(it) }
            followers.forEach { this.storage.storeUser(it) }
            tweets.forEach { this.storage.storeTweet(it) }
            this.storage.storeUserConnections(userDownload.userId,
                followees.map { it.id }.toMutableList(),
                followers.map { it.id }.toMutableList(),
                tweets.map { it.id }.toMutableList())
            this.storage.removeUserDownload(userDownload)
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

    private fun storeTweet(tweet: Status){
        LOGGER.debug("Processing tweet: {}", tweet.id)
        if (this.storage.storeTweet(tweet)) {
            val urls = mutableSetOf<String>()
            urls.addAll(tweet.urlEntities.map { it.expandedURL })
            urls.addAll(tweet.mediaEntities.map { it.expandedURL })
            this.storage.storeUserDownload(tweet.user.id)
            urls.forEach { this.storage.storeUrlDownload(it, tweet.id) }
        }
    }
}