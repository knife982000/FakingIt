package edu.isistan.fakenews.crawler

import org.slf4j.LoggerFactory
import edu.isistan.fakenews.storage.MongoDBStorage

import twitter4j.*
import java.lang.Thread.sleep
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.thread
import kotlin.concurrent.withLock
import kotlin.system.exitProcess

var MAX_SAVE = 100

class TwitterStreamer(val storage: MongoDBStorage) {

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


    fun trackTopics(
        topics: Array<String>?,
        locations: Array<DoubleArray>?,
        lang: Array<String>?,
        max_statuses: Int
    ) {

        LOGGER.info("Tracking users: ${topics.toString()}, ${lang.toString()}, ${locations.toString()}")

        val lock = Object()
        val counter = AtomicInteger()
        var tweetFilterQuery: FilterQuery? = null

        if (topics != null || lang != null || locations != null) {
            tweetFilterQuery = FilterQuery()
            if (topics != null) tweetFilterQuery.track(*topics)
            if (lang != null) tweetFilterQuery.language(*lang)
            if (locations != null) tweetFilterQuery.locations(*locations)

        }

        this.retryTwitterStreamWrapper {
            trackQuery(false, tweetFilterQuery, counter, max_statuses, lock)
        }
    }

    fun track(
        topics: Array<String>?,
        locations: Array<DoubleArray>?,
        lang: Array<String>?,
        users: MutableSet<Long>?,
        max_statuses: Int
    ): String {

        LOGGER.info("Tracking users: ${topics.toString()}, ${lang.toString()}, ${locations.toString()}")

        val lock = Object()
		val counter = AtomicInteger()
        var tweetFilterQuery: FilterQuery? = null

        if (topics != null || lang != null || locations != null || users != null) {
            tweetFilterQuery = FilterQuery()
            if (topics != null) tweetFilterQuery.track(*topics)
            if (lang != null) tweetFilterQuery.language(*lang)
            if (locations != null) tweetFilterQuery.locations(*locations)
            if (users != null) tweetFilterQuery.follow(*users.toLongArray())
        }

		var query = ""
        this.retryTwitterStreamWrapper {
			query = trackQuery(true, tweetFilterQuery, counter, max_statuses, lock)
        }
		return query
    }

    private fun getListener(
        user: Boolean,
        storageHelper: StorageHelper,
        max_statuses: Int,
        lock: Object
    ): StreamListener {

        if (user) {
            val userListener = object : UserStreamListener {

                override fun onStatus(status: Status) {
                    processStatus(storageHelper, status, max_statuses, lock)
                }

                override fun onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}

                override fun onTrackLimitationNotice(numberOfLimitedStatuses: Int) {
                    LOGGER.warn("Track Limitation Notice: {$numberOfLimitedStatuses}")
                }

                override fun onStallWarning(warning: StallWarning) {
                    LOGGER.warn(warning.getMessage())
                }

                override fun onScrubGeo(userId: Long, upToStatusId: Long) {}

                override fun onException(ex: Exception) {
                    LOGGER.error("Error with streamer! ${ex.getLocalizedMessage()}")
					nextTwitterConnection(null)	
                }

                override fun onBlock(source: User, blockedUser: User) {}
                override fun onDeletionNotice(directMessageId: Long, userId: Long) {}
                override fun onDirectMessage(directMessage: DirectMessage) {}

                override fun onFavorite(source: User, target: User, favoritedStatus: Status) {
					processStatus(storageHelper, favoritedStatus, max_statuses, lock)
                }

                override fun onFavoritedRetweet(
                    source: User,
                    target: User,
                    favoritedRetweeet: Status
                ) {
                } // processStatus(statuses,statuses_ids,favoritedRetweeet,max_statuses,lock) }

                override fun onFollow(source: User, followedUser: User) {}
                override fun onFriendList(friendIds: LongArray) {}

                override fun onQuotedTweet(source: User, target: User, quotingTweet: Status) {
					processStatus(storageHelper, quotingTweet, max_statuses, lock)
                }

                override fun onRetweetedRetweet(
                    source: User,
                    target: User,
                    retweetedStatus: Status
                ) {
                } //processStatus(statuses,statuses_ids,retweetedStatus,max_statuses,lock) }

                override fun onUnblock(source: User, unblockedUser: User) {}
                override fun onUnfavorite(source: User, target: User, unfavoritedStatus: Status) {}
                override fun onUnfollow(source: User, unfollowedUser: User) {}
                override fun onUserDeletion(deletedUser: Long) {}
                override fun onUserListCreation(listOwner: User, list: UserList) {}
                override fun onUserListDeletion(listOwner: User, list: UserList) {}
                override fun onUserListMemberAddition(addedMember: User, listOwner: User, list: UserList) {}
                override fun onUserListMemberDeletion(deletedMember: User, listOwner: User, list: UserList) {}
                override fun onUserListSubscription(subscriber: User, listOwner: User, list: UserList) {}
                override fun onUserListUnsubscription(subscriber: User, listOwner: User, list: UserList) {}
                override fun onUserListUpdate(listOwner: User, list: UserList) {}
                override fun onUserProfileUpdate(updatedUser: User) {}
                override fun onUserSuspension(suspendedUser: Long) {}
            }
            return userListener
        }

        val statusListener = object : StatusListener {

            override fun onStatus(status: Status) {
				processStatus(storageHelper, status, max_statuses, lock)
            }

            override fun onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}

            override fun onTrackLimitationNotice(numberOfLimitedStatuses: Int) {
                LOGGER.warn("Track Limitation Notice: {$numberOfLimitedStatuses}")
            }

            override fun onStallWarning(warning: StallWarning) {
                LOGGER.warn(warning.getMessage())
            }

            override fun onScrubGeo(userId: Long, upToStatusId: Long) {}

            override fun onException(ex: Exception) {
                LOGGER.error(ex.getLocalizedMessage())
                synchronized(lock) {
                    lock.notify();
                }
            }

        }
        return statusListener
    }

    fun trackUsers(users: LongArray, max_statuses: Int) {

        LOGGER.info("Tracking users: ${users.toString()}")

        val lock = Object()
        val counter = AtomicInteger()

        val tweetFilterQuery = FilterQuery()
        tweetFilterQuery.follow(*users)

        trackQuery(true, tweetFilterQuery, counter, max_statuses, lock)

    }

    private fun trackQuery(
        user: Boolean,
        tweetFilterQuery: FilterQuery?,
        counter: AtomicInteger,
        max_statuses: Int,
        lock: Object
    ): String {

        val query = tweetFilterQuery?.toString() ?: "random sampling"

        val storageHelper = StorageHelper(storage, query, counter)

        twitterStr.addListener(getListener(user, storageHelper, max_statuses, lock))

        thread(name = "StorageHelper") { storageHelper.run() }
        if (tweetFilterQuery != null)
            twitterStr.filter(tweetFilterQuery)
        else
            twitterStr.sample()

        try {
            synchronized(lock) {
                while (counter.get() < max_statuses)
                    lock.wait();
            }
        } catch (e: InterruptedException) {
            LOGGER.error(e.getLocalizedMessage())
        }

        twitterStr.shutdown();
        storageHelper.stop()

        LOGGER.debug("Total stored tweets: ${counter.get()}")
		return query
    }

    //maybe it is not needed to keep the statuses_ids as an structure given that they are going to be added in each MAX_SAVE
    private fun processStatus(
        storageHelper: StorageHelper,
        status: Status,
        max_statuses: Int,
        lock: Object
    ) {
        storageHelper.storeOrIgnore(status)
        if (storageHelper.counter.get() > max_statuses) {
            synchronized(lock) {
                lock.notify();
            }
        }
    }

    private fun retryTwitterStreamWrapper(action: () -> Unit) {
        assert(this::twitterStr.isInitialized) { "TwitterStream was not initialized" }
        var completed = false
        var networkFails = 0
        while (!completed) {
            completed = try {
                action()
                networkFails = 0
                true
            } catch (e: TwitterException) {
                LOGGER.warn(
                    "Attempting to recovery from an error. Number of network fails {}. Error: {}",
                    networkFails,
                    e.errorMessage
                )
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

/**
 * Helper that store or ignore an status. Helps to reduce memory
 * consumption on high throughput scenarios.
 */
private class StorageHelper(
    val storage: MongoDBStorage,
    val query: String,
    val counter: AtomicInteger
) : Runnable {
    var running = true
	var completed = false
    val lock = ReentrantLock()
    val condition = lock.newCondition()
    val data = LinkedList<Status>()
    val maxQueueSize = System.getProperty("stream.max_queue_size", "10000").toInt()

    fun storeOrIgnore(status: Status): Boolean {
        lock.withLock {
            if (data.size < maxQueueSize && running) {
                data.add(status)
                condition.signal()
                return true
            }
        }
        return false
    }

    fun nextStatus(): Status? {
        lock.withLock {
            while (running && data.isEmpty()) {
                condition.await()
            }
            return if (data.isNotEmpty())
                data.removeAt(0)
            else
                null
        }
    }

    fun stop() {
        lock.withLock {
            running = false
			condition.signal()
            while (!completed) {
                condition.await()
            }
        }
    }

    override fun run() {
        val statuses = mutableSetOf<Status>()
        while (true) {
            val status = nextStatus()
            if (status != null) {
                statuses.add(status)

                if (statuses.size > MAX_SAVE) {
                    storeStatuses(statuses)
                }
                counter.incrementAndGet()
            } else {
                if (statuses.isNotEmpty()) {
                    storeStatuses(statuses)
                }
				lock.withLock {
					completed = true
					condition.signal()
				}
                return
            }
        }
    }

    private fun storeStatuses(statuses: MutableSet<Status>) {
        statuses.forEach { storeTweet(it) }
        this.storage.findOrStoreQuery(
            query,
            statuses.map { it.id }.toMutableList()
        ) //update the query with the new ids
        statuses.clear()
    }

    private fun storeTweet(tweet: Status) {
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
}

fun main() {

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

    streamer.track(topic, locations, lang, userIds, max)

    storage.close()
}