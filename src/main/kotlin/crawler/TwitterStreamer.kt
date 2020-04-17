package edu.isistan.fakenews.crawler

import org.slf4j.LoggerFactory
import edu.isistan.fakenews.storage.MongoDBStorage
import edu.isistan.fakenews.crawler.TwitterCrawler

import edu.isistan.fakenews.crawler.ConfigurationActivation
import edu.isistan.fakenews.crawler.TwitterAuth

import edu.isistan.fakenews.crawler.createConfiguration
import edu.isistan.fakenews.crawler.loadConfigurations

import edu.isistan.fakenews.storage.DEBUG_DB

import twitter4j.*
import java.lang.Thread.sleep
import kotlin.system.exitProcess

var MAX_SAVE = 100

class TwitterStreamer(val storage: MongoDBStorage){
	
	var LOGGER = LoggerFactory.getLogger(TwitterStreamer::class.java)!!
		
	private val configurations = sortedSetOf<ConfigurationActivation>()
	private lateinit var currentConfiguration: TwitterAuth

	lateinit var twitterStr: TwitterStream

	init {
				LOGGER.info("Initializing twitter crawler")
				configurations.addAll(loadConfigurations())
				this.nextTwitterConnection(null)
			}
	
	//technically there is no limit on the stream api, nonetheless, if there are many connections you can get limited... 
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
			twitterStr = TwitterStreamFactory(currentConfiguration.createConfiguration(true)).instance
	}
	
		
	fun trackTopics(topics : Array<String>?, locations : Array<DoubleArray>?, lang : Array<String>?, max_statuses : Int) : MutableSet<Long>{
		
		LOGGER.info("Tracking users: ${topics.toString()}, ${lang.toString()}, ${locations.toString()}")
		
		val lock = Object()
		val statuses = mutableSetOf<Status>()
		val statuses_ids = mutableSetOf<Long>()				
		var tweetFilterQuery : FilterQuery? = null
		
		if(topics != null || lang != null || locations != null){
			tweetFilterQuery = FilterQuery()
			if(topics != null) tweetFilterQuery.track(*topics)
			if(lang != null) tweetFilterQuery.language(*lang)
			if(locations != null) tweetFilterQuery.locations(*locations)
			
		}
		
		this.retryTwitterStreamWrapper{	
			trackQuery(false,tweetFilterQuery,statuses,statuses_ids,max_statuses,lock)
		}
		
		return statuses_ids
	}
	
	fun track(topics : Array<String>?, locations : Array<DoubleArray>?, lang : Array<String>?, users : MutableSet<Long>?, max_statuses : Int) : MutableSet<Long>{
		
		LOGGER.info("Tracking users: ${topics.toString()}, ${lang.toString()}, ${locations.toString()}")
		
		val lock = Object()
		val statuses = mutableSetOf<Status>()
		val statuses_ids = mutableSetOf<Long>()				
		var tweetFilterQuery : FilterQuery? = null
		
		if(topics != null || lang != null || locations != null || users != null){
			tweetFilterQuery = FilterQuery()
			if(topics != null) tweetFilterQuery.track(*topics)
			if(lang != null) tweetFilterQuery.language(*lang)
			if(locations != null) tweetFilterQuery.locations(*locations)
			if(users != null) tweetFilterQuery.follow(*users.toLongArray())
		}
		
		this.retryTwitterStreamWrapper{	
			trackQuery(true,tweetFilterQuery,statuses,statuses_ids,max_statuses,lock)
		}
		
		return statuses_ids
	}
	
	private fun getListener(user : Boolean, statuses : MutableSet<Status>, statuses_ids : MutableSet<Long>, max_statuses : Int, query : String, lock : Object) : StreamListener{
		
		if(user){
			val userListener = object : UserStreamListener{
			
			override fun onStatus(status : Status) { processStatus(statuses,statuses_ids,status,max_statuses,query,lock) }
			
			override fun onDeletionNotice(statusDeletionNotice : StatusDeletionNotice){}
			
			override fun onTrackLimitationNotice(numberOfLimitedStatuses : Int){ LOGGER.warn("Track Limitation Notice: {$numberOfLimitedStatuses}") }
			
			override fun onStallWarning(warning : StallWarning){ LOGGER.warn(warning.getMessage()) }
			
			override fun onScrubGeo(userId : Long,upToStatusId : Long){}
			
			override fun onException(ex : Exception){ LOGGER.error(ex.getLocalizedMessage()) }
			
			override fun onBlock(source : User, blockedUser : User) {}
			override fun onDeletionNotice(directMessageId : Long, userId : Long) {} 
			override fun onDirectMessage(directMessage : DirectMessage) {}
			
			override fun onFavorite(source : User, target : User , favoritedStatus : Status) { processStatus(statuses,statuses_ids,favoritedStatus,max_statuses,query,lock) }
			
			override fun onFavoritedRetweet(source : User, target : User, favoritedRetweeet : Status) {} // processStatus(statuses,statuses_ids,favoritedRetweeet,max_statuses,lock) }
			
			override fun onFollow(source : User, followedUser : User) {}
			override fun onFriendList(friendIds : LongArray) {}
			
			override fun onQuotedTweet(source : User, target : User, quotingTweet : Status) { processStatus(statuses,statuses_ids,quotingTweet,max_statuses,query,lock) }
			override fun onRetweetedRetweet(source : User, target : User, retweetedStatus : Status) {} //processStatus(statuses,statuses_ids,retweetedStatus,max_statuses,lock) }
			
			override fun onUnblock(source : User, unblockedUser : User) {}
			override fun onUnfavorite(source : User, target : User, unfavoritedStatus : Status) {}
			override fun onUnfollow(source : User, unfollowedUser : User) {}
			override fun onUserDeletion(deletedUser : Long) {}
			override fun onUserListCreation(listOwner : User, list : UserList) {}
			override fun onUserListDeletion(listOwner : User, list : UserList) {}
			override fun onUserListMemberAddition(addedMember : User, listOwner : User, list : UserList) {}
			override fun onUserListMemberDeletion(deletedMember : User, listOwner : User, list : UserList) {}
			override fun onUserListSubscription(subscriber : User, listOwner : User, list : UserList) {}
			override fun onUserListUnsubscription(subscriber : User, listOwner : User, list : UserList) {}
			override fun onUserListUpdate(listOwner : User, list : UserList) {}
			override fun onUserProfileUpdate(updatedUser : User) {}
			override fun onUserSuspension(suspendedUser : Long) {}
		}
		return userListener
		}
		
		val statusListener = object : StatusListener{
			
			override fun onStatus(status : Status) { processStatus(statuses,statuses_ids,status,max_statuses,query,lock) }
			
			override fun onDeletionNotice(statusDeletionNotice : StatusDeletionNotice){}
			
			override fun onTrackLimitationNotice(numberOfLimitedStatuses : Int){ LOGGER.warn("Track Limitation Notice: {$numberOfLimitedStatuses}") }
			
			override fun onStallWarning(warning : StallWarning){ LOGGER.warn(warning.getMessage()) }
				
			override fun onScrubGeo(userId : Long,upToStatusId : Long){}
			
			override fun onException(ex : Exception){
				LOGGER.error(ex.getLocalizedMessage())
				synchronized (lock) {
						lock.notify();
					}
			}
			
		}
		return statusListener
	}
	
	fun trackUsers(users : LongArray, max_statuses : Int) : MutableSet<Long>{
		
		LOGGER.info("Tracking users: ${users.toString()}")
		
		val lock = Object()
		val statuses = mutableSetOf<Status>()
		val statuses_ids = mutableSetOf<Long>()
		
		val tweetFilterQuery = FilterQuery()
		tweetFilterQuery.follow(*users)
		
		trackQuery(true,tweetFilterQuery,statuses,statuses_ids,max_statuses,lock)
		
		return statuses_ids
	}
	
	private fun trackQuery(user : Boolean, tweetFilterQuery : FilterQuery?, statuses : MutableSet<Status>, statuses_ids : MutableSet<Long>, max_statuses : Int, lock : Object){
		
		val query = tweetFilterQuery?.toString() ?: "random sampling"

		twitterStr.addListener(getListener(user,statuses, statuses_ids,max_statuses,query,lock))
		
		if(tweetFilterQuery != null)
			twitterStr.filter(tweetFilterQuery)
		else
			twitterStr.sample()
		
		try {
			synchronized (lock) {
				while(statuses_ids.size < max_statuses)
					lock.wait();
			}
		} catch (e : InterruptedException) {
			LOGGER.error(e.getLocalizedMessage())
		}
		
		twitterStr.shutdown();
	
		if(statuses.size > 0) {
			statuses.forEach{ storeTweet(it) }
			this.storage.findOrStoreQuery(query,statuses.map{it.getId()}.toMutableList())
		}
		
		LOGGER.debug("${statuses_ids.size}")
		
//		if(tweetFilterQuery != null)
//			this.storage.findOrStoreQuery(tweetFilterQuery.toString(),statuses_ids.toMutableList())
//		else
//			this.storage.findOrStoreQuery("random sampling",statuses_ids.toMutableList())
	}
	
	//the same as in TwitterCrawler
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
		//maybe it is not needed to keep the statuses_ids as an structure given that they are going to be added in each MAX_SAVE
	private fun processStatus(statuses : MutableSet<Status>, statuses_ids : MutableSet<Long>, status : Status,max_statuses : Int, query : String, lock : Object){
		statuses.add(status);
		statuses_ids.add(status.getId())
				
		if(statuses.size > MAX_SAVE){
			statuses.forEach{ storeTweet(it) }
			this.storage.findOrStoreQuery(query,statuses.map{it.id}.toMutableList()) //update the query with the new ids
			statuses.clear()
		}
			
		synchronized (lock) {	
			if (statuses_ids.size > max_statuses) 
				lock.notify();
		}
//				else
//					statuses.add(status)
	}
	
	private fun retryTwitterStreamWrapper(action: ()->Unit) {
	assert(this::twitterStr.isInitialized) { "TwitterStream was not initialized" }
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

	
}

fun main(){
		
		DEBUG_DB = true
		val storage = MongoDBStorage()
		
		val streamer = TwitterStreamer(storage)
		
		val topic = arrayOf<String>("stanford")
		val locations = null //arrayOf<DoubleArray>()//doubleArrayOf(-0.489,51.28),doubleArrayOf(0.236,51.686))
		
		val lang = null //arrayOf<String>()
		val max = 50
	
		val userIds = mutableSetOf<Long>()
		userIds.add(69416519)
		userIds.add(33989170)
		
//		print(streamer.trackTopics(topic,locations,lang,max))
	
//		streamer.trackUsers(users,max)
	
		streamer.track(topic,locations,lang,userIds,max)
	
		storage.close()
	}