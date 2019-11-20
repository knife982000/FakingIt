package edu.isistan.fakenews.storage

import com.mongodb.MongoClientSettings
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.Filters
import com.mongodb.client.model.IndexOptions
import com.mongodb.client.model.Indexes
import com.mongodb.client.model.Sorts
import org.bson.BsonObjectId
import org.bson.codecs.BsonObjectIdCodec
import org.bson.codecs.configuration.CodecRegistries
import org.bson.codecs.configuration.CodecRegistry
import org.bson.codecs.pojo.ClassModel
import org.bson.codecs.pojo.PojoCodecProvider
import org.bson.codecs.pojo.PropertyModelBuilder
import org.bson.types.ObjectId
import org.slf4j.LoggerFactory
import twitter4j.Status
import java.io.Closeable

var DEBUG_DB = false
val FAKE_NEWS_DB by lazy { if (DEBUG_DB) "FakeNewsTest" else "FakeNews" }


const val TWEETS_COLLECTION = "tweets"
const val USERS_COLLECTION = "users"
const val PLACES_COLLECTION = "places"
const val QUERIES_COLLECTION = "queries"
const val USER_CONNECTIONS_COLLECTION = "userConnections"
const val QUERY_DOWNLOAD_COLLECTION = "queryDownload"
const val URL_DOWNLOAD_COLLECTION = "urlDownload"
const val USER_DOWNLOAD_COLLECTION = "userDownload"

const val TWEET_ID = "tweetId"
const val USER_ID = "userId"
const val PLACE_ID = "placeId"
const val QUERY_TEXT = "text"
const val QUERY_MAX_ID = "maxId"
const val URL = "url"


const val TWEET_ID_INDEX = "TWEET_ID_INDEX"
const val USER_ID_INDEX = "USER_ID_INDEX"
const val PLACE_ID_INDEX = "PLACE_ID_INDEX"
const val QUERY_TEXT_INDEX = "QUERY_TEXT_INDEX"
const val QUERY_MAX_ID_INDEX = "MAX_ID_INDEX"
const val URL_INDEX = "URL_INDEX"


private val MONGO_ID = "_id"

var LOGGER = LoggerFactory.getLogger(MongoDBStorage::class.java)!!

/**
 * Converts a Status from Twitter4J into a Tweet object for storage
 */
fun Status.toStorage(): Tweet<ObjectId> {
    return Tweet(null, this.id, this.user?.id, this.text, this.createdAt, this.source, this.displayTextRangeStart,
        this.displayTextRangeEnd, this.isTruncated, this.inReplyToStatusId, this.inReplyToUserId, this.inReplyToScreenName,
        this.geoLocation.toStorage(), this.place?.id, this.favoriteCount, this.isRetweet, this.currentUserRetweetId,
        this.contributors?.toMutableList(),this.retweetCount, this.isPossiblySensitive, this.lang, this.quotedStatusId,
        this.scopes?.placeIds?.toMutableList(), this.withheldInCountries?.toMutableList(),
        this.userMentionEntities.map { it.toStorage() }.toMutableList(),
        this.urlEntities.map { it.toStorage() }.toMutableList(), this.mediaEntities.map { it.toStorage() }.toMutableList(),
        this.hashtagEntities.map { it.toStorage() }.toMutableList())
}

/**
 * Converts a GeoLocation from Twitter4J into a Tweet object for storage
 */
fun twitter4j.GeoLocation?.toStorage(): GeoLocation? {
    if (this == null)
        return null
    return GeoLocation(this.latitude, this.longitude)
}

/**
 * Converts an UserMentionEntity from Twitter4J into an object for storage
 */
fun twitter4j.UserMentionEntity.toStorage(): UserEntity {
    return UserEntity(this.id, this.start, this.end)
}

/**
 * Converts a URLEntity from Twitter4J into an object for storage
 */
fun twitter4j.URLEntity.toStorage(): URLEntity {
    return URLEntity(this.url, this.expandedURL, this.displayURL, this.start, this.end)
}

/**
 * Converts a MediaEntity from Twitter4J into an object for storage
 */
fun twitter4j.MediaEntity.toStorage(): MediaEntity {
    return MediaEntity(this.url, this.expandedURL, this.displayURL, this.start, this.end, this.id,
        this.mediaURL, this.mediaURLHttps, this.type, this.extAltText)
}

/**
 * Converts a HashtagEntity from Twitter4J into an object for storage
 */
fun twitter4j.HashtagEntity.toStorage(): BaseEntity {
    return BaseEntity(this.start, this.end)
}

/**
 * Converts an User from Twitter4J into an object for storage
 */
fun twitter4j.User.toStorage(): User<ObjectId> {
    return User(null, this.id, this.name, this.email, this.screenName, this.createdAt, this.location,
        this.description, this.isContributorsEnabled, this.profileImageURL, this.biggerProfileImageURL,
        this.miniProfileImageURL, this.originalProfileImageURL, this.get400x400ProfileImageURL(),
        this.profileImageURLHttps, this.biggerProfileImageURLHttps, this.miniProfileImageURLHttps,
        this.originalProfileImageURLHttps, this.get400x400ProfileImageURLHttps(), this.isDefaultProfileImage,
        this.url, this.isProtected, this.followersCount, this.status?.id)
}

/**
 * Converts a Place from Twitter4J into an object for storage
 */
fun twitter4j.Place.toStorage(): Place<ObjectId> {
    return Place(null, this.id, this.url, this.placeType, this.name, this.fullName, this.country,
        toStorage(this.boundingBoxType, this.boundingBoxCoordinates),
        this.containedWithIn?.map { it.id }?.toMutableList())
}

/**
 * Create a boundingBox
 */
fun toStorage(type: String, geodata: Array<Array<twitter4j.GeoLocation>>): BoundingBox{
    return BoundingBox(geodata.flatMap { it.map { g-> GeoLocation(g.latitude, g.longitude) } }.toMutableList(), type)
}

/**
 * This class provides support for storing the information into a MongoDB database
 */
class MongoDBStorage: AutoCloseable, Closeable {
    lateinit var client: MongoClient
    lateinit var database: MongoDatabase
    lateinit var tweets: MongoCollection<Tweet<ObjectId>>
    lateinit var users: MongoCollection<User<ObjectId>>
    lateinit var places: MongoCollection<Place<ObjectId>>
    lateinit var queries: MongoCollection<Query<ObjectId>>
    lateinit var userConnections: MongoCollection<UserConnections<ObjectId>>
    lateinit var queryDownloads: MongoCollection<QueryDownload<ObjectId>>
    lateinit var urlDownloads: MongoCollection<URLDownload<ObjectId>>
    lateinit var userDownloads: MongoCollection<UserDownload<ObjectId>>

    /**
     * Initialize the connection to the mongodb database
     */
    fun init(){
        val codecRegistry = createCodecRegistries(Tweet::class.java,
                                                  Place::class.java,
                                                  User::class.java,
                                                  Query::class.java,
                                                  UserConnections::class.java,
                                                  QueryDownload::class.java,
                                                  URLDownload::class.java,
                                                  UserDownload::class.java)
        LOGGER.info("Initialing the database")
        //TODO: Accept connection configuration through a file. Currently only supports a local MongoDb without user
        this.client = MongoClients.create()
        this.database = this.client.getDatabase(FAKE_NEWS_DB)
        //Connects to the different collections
        twitterCollections(codecRegistry)
        downloadCollections(codecRegistry)
        LOGGER.info("Initialing indexes")
        twitterIndexes()
        downloadIndexes()
    }

    fun storeUser(user: twitter4j.User): Boolean {
        LOGGER.debug("Inserting user {}", user.id)
        val sUser = user.toStorage()
        if (this.users.countDocuments(Filters.eq(USER_ID, sUser.userId)) != 0L) {
            LOGGER.debug("User {} was present", user.id)
            return false
        }
        val sTweet = user.status?.toStorage()
        if (sTweet!=null && this.tweets.countDocuments(Filters.eq(TWEET_ID, sTweet.tweetId)) == 0L)
            this.tweets.insertOne(sTweet)
        this.users.insertOne(sUser)
        return true
    }

    fun storePlace(place: twitter4j.Place): Boolean {
        LOGGER.debug("Inserting place {}", place.id)
        val sPlace = place.toStorage()
        if (this.places.countDocuments(Filters.eq(PLACE_ID, sPlace.placeId)) != 0L) {
            LOGGER.debug("Place {} was present", place.id)
            return false
        }
        this.places.insertOne(sPlace)
        return true
    }

    fun storeTweet(tweet: Status): Boolean {
        LOGGER.debug("Inserting tweet {}", tweet.id)
        val sTweet = tweet.toStorage()
        if (this.tweets.countDocuments(Filters.eq(TWEET_ID, sTweet.tweetId)) != 0L) {
            LOGGER.debug("Tweet {} was present", tweet.id)
            return false
        }
        if (tweet.user != null)
            this.storeUser(tweet.user)
        if (tweet.place != null)
            this.storePlace(tweet.place)
        this.tweets.insertOne(sTweet)
        return true
    }

    fun findOrStoreQuery(query: String): Query<ObjectId> {
        LOGGER.debug("Searching Query {}", query)
        var q = this.queries.find(Filters.eq(QUERY_TEXT, query)).first()
        if (q != null)
            return q
        LOGGER.debug("The Query {} not found, creating a new one", query)
        q = Query(null, query, mutableListOf())
        this.queries.insertOne(q)
        return q
    }

    fun findOrStoreQuery(query: Query<ObjectId>): Query<ObjectId> {
        LOGGER.debug("Persisting Query {}", query)
        if (query.id == null) {
            this.queries.insertOne(query)
            query.id = this.findOrStoreQuery(query.text).id //WorkAround BUG: https://jira.mongodb.org/browse/JAVA-2674
        } else {
            this.queries.replaceOne(Filters.eq(MONGO_ID, query.id), query)
        }
        return query
    }

    fun findOrStoreQueryDownload(queryDownload: String): QueryDownload<ObjectId> {
        LOGGER.debug("Searching Query Download {}", queryDownload)
        var q = this.queryDownloads.find(Filters.eq(QUERY_TEXT, queryDownload)).first()
        if (q != null)
            return q
        LOGGER.debug("The Query Download {} not found, creating a new one", queryDownload)
        q = QueryDownload(null, queryDownload, -1L)
        this.queryDownloads.insertOne(q)
        return q
    }

    fun findOrStoreQueryDownload(queryDownload: QueryDownload<ObjectId>): QueryDownload<ObjectId> {
        LOGGER.debug("Searching Query Download {}", queryDownload)
        if (queryDownload.id == null) {
            this.queryDownloads.insertOne(queryDownload)
            queryDownload.id = this.findOrStoreQueryDownload(queryDownload.text).id //WorkAround BUG: https://jira.mongodb.org/browse/JAVA-2674
        } else {
            this.queryDownloads.replaceOne(Filters.eq(MONGO_ID, queryDownload.id), queryDownload)
        }
        return queryDownload
    }

    fun nextQueryDownload(): QueryDownload<ObjectId>? {
        LOGGER.debug("Getting next Query Download")
        return this.queryDownloads.find().sort(Sorts.descending(QUERY_MAX_ID)).first()
    }

    fun removeQueryDownload(queryDownload: QueryDownload<ObjectId>): Boolean {
        LOGGER.debug("Removing Query Download {}", queryDownload)
        if (queryDownload.id == null)
            return false
        return this.queryDownloads.deleteOne(Filters.eq(MONGO_ID, queryDownload.id)).wasAcknowledged()
    }


    fun storeUserDownload(userId: Long): Boolean {
        if (this.userDownloads.countDocuments(Filters.eq(USER_ID, userId)) != 0L ||
                this.userConnections.countDocuments(Filters.eq(USER_ID, userId)) != 0L)
            return false
        val userDownload = UserDownload<ObjectId>(null, userId)
        this.userDownloads.insertOne(userDownload)
        return true
    }

    fun storeUrlDownload(url: String, tweetId: Long) {
        val urlDownload = URLDownload<ObjectId>(null, url, tweetId)
        this.urlDownloads.insertOne(urlDownload)
    }

    private fun downloadIndexes() {
        if (this.queryDownloads.listIndexes().find { it.getString("name") == QUERY_TEXT_INDEX } == null) {
            LOGGER.info("Creating $QUERY_TEXT_INDEX for $QUERY_DOWNLOAD_COLLECTION")
            this.queryDownloads.createIndex(Indexes.text(QUERY_TEXT), IndexOptions().name(QUERY_TEXT_INDEX))
        }
        if (this.queryDownloads.listIndexes().find { it.getString("name") == QUERY_MAX_ID_INDEX } == null) {
            LOGGER.info("Creating $QUERY_MAX_ID_INDEX for $QUERY_DOWNLOAD_COLLECTION")
            this.queryDownloads.createIndex(Indexes.descending(QUERY_MAX_ID), IndexOptions().name(QUERY_MAX_ID_INDEX))
        }
        if (this.urlDownloads.listIndexes().find { it.getString("name") == URL_INDEX } == null) {
            LOGGER.info("Creating $URL_INDEX for $URL_DOWNLOAD_COLLECTION")
            this.urlDownloads.createIndex(Indexes.hashed(URL), IndexOptions().name(URL_INDEX))
        }
        if (this.userDownloads.listIndexes().find { it.getString("name") == USER_ID_INDEX } == null) {
            LOGGER.info("Creating $USER_ID_INDEX for $USER_DOWNLOAD_COLLECTION")
            this.userDownloads.createIndex(Indexes.hashed(USER_ID), IndexOptions().name(USER_ID_INDEX))
        }

    }

    fun nextUserDownload(): UserDownload<ObjectId>? {
        return this.userDownloads.find().first()
    }

    fun storeUserConnections(userId: Long, followees: MutableList<Long>, followers: MutableList<Long>, tweets: MutableList<Long>) {
        var userConnections = this.userConnections.find(Filters.eq(USER_ID, userId)).first()
        if (userConnections != null) {
            userConnections.followees.addAll(followees.filter { it !in userConnections.followees })
            userConnections.followers.addAll(followers.filter { it !in userConnections.followers })
            userConnections.tweets.addAll(tweets.filter { it !in userConnections.tweets })
            this.userConnections.replaceOne(Filters.eq(USER_ID, userId), userConnections)
            return
        }
        userConnections = UserConnections(null, userId, followees, followers, tweets)
        this.userConnections.insertOne(userConnections)
    }

    fun removeUserDownload(userDownload: UserDownload<ObjectId>): Boolean {
        if (userDownload.id == null)
            return false
        return this.userDownloads.deleteOne(Filters.eq(MONGO_ID, userDownload.id)).wasAcknowledged()
    }

    override fun close() {
        LOGGER.info("Closing connection to the database")
        this.client.close()
    }

    private fun twitterIndexes() {
        if (this.tweets.listIndexes().find { it.getString("name") == TWEET_ID_INDEX } == null) {
            LOGGER.info("Creating $TWEET_ID_INDEX for $TWEETS_COLLECTION")
            this.tweets.createIndex(Indexes.hashed(TWEET_ID), IndexOptions().name(TWEET_ID_INDEX))
        }
        if (this.users.listIndexes().find { it.getString("name") == USER_ID_INDEX } == null) {
            LOGGER.info("Creating $USER_ID_INDEX for $USERS_COLLECTION")
            this.users.createIndex(Indexes.hashed(USER_ID), IndexOptions().name(USER_ID_INDEX))
        }
        if (this.places.listIndexes().find { it.getString("name") == PLACE_ID_INDEX } == null) {
            LOGGER.info("Creating $PLACE_ID_INDEX for $PLACES_COLLECTION")
            this.places.createIndex(Indexes.hashed(PLACE_ID), IndexOptions().name(PLACE_ID_INDEX))
        }
        if (this.queries.listIndexes().find { it.getString("name") == QUERY_TEXT_INDEX } == null) {
            LOGGER.info("Creating $QUERY_TEXT_INDEX for $QUERIES_COLLECTION")
            this.queries.createIndex(Indexes.text(QUERY_TEXT), IndexOptions().name(QUERY_TEXT_INDEX))
        }
        if (this.userConnections.listIndexes().find { it.getString("name") == USER_ID_INDEX } == null) {
            LOGGER.info("Creating $USER_ID_INDEX for $USER_CONNECTIONS_COLLECTION")
            this.userConnections.createIndex(Indexes.hashed(USER_ID), IndexOptions().name(USER_ID_INDEX))
        }
    }

    private fun downloadCollections(codecRegistry: CodecRegistry) {
        this.queryDownloads = this.database.getCollection(
            QUERY_DOWNLOAD_COLLECTION,
            QueryDownload::class.java
        ).withCodecRegistry(codecRegistry) as MongoCollection<QueryDownload<ObjectId>>
        this.urlDownloads = this.database.getCollection(
            URL_DOWNLOAD_COLLECTION,
            URLDownload::class.java
        ).withCodecRegistry(codecRegistry) as MongoCollection<URLDownload<ObjectId>>
        this.userDownloads = this.database.getCollection(
            USER_DOWNLOAD_COLLECTION,
            UserDownload::class.java
        ).withCodecRegistry(codecRegistry) as MongoCollection<UserDownload<ObjectId>>
    }

    private fun twitterCollections(codecRegistry: CodecRegistry) {
        this.tweets = this.database.getCollection(
            TWEETS_COLLECTION,
            Tweet::class.java
        ).withCodecRegistry(codecRegistry) as MongoCollection<Tweet<ObjectId>>
        this.users = this.database.getCollection(
            USERS_COLLECTION,
            User::class.java
        ).withCodecRegistry(codecRegistry) as MongoCollection<User<ObjectId>>
        this.places = this.database.getCollection(
            PLACES_COLLECTION,
            Place::class.java
        ).withCodecRegistry(codecRegistry) as MongoCollection<Place<ObjectId>>
        this.queries = this.database.getCollection(
            QUERIES_COLLECTION,
            Query::class.java
        ).withCodecRegistry(codecRegistry) as MongoCollection<Query<ObjectId>>
        this.userConnections =this.database.getCollection(
            USER_CONNECTIONS_COLLECTION,
            UserConnections::class.java
        ).withCodecRegistry(codecRegistry) as MongoCollection<UserConnections<ObjectId>>
    }
}

/**
 * Creates codecs regitries as a work around of the inability of plain PojoCodecProvider
 * to Serialize/Unserialize generic classes
 */
private fun createCodecRegistries(vararg classes: Class<*>): CodecRegistry {
    LOGGER.info("Initializing Codec Registry")
    val pojoCodecProvider = PojoCodecProvider.builder()
    for (clazz in classes) {
        LOGGER.debug("Adding {} to the CodecRegistry", clazz.name)
        val cmb = ClassModel.builder(clazz)
        val c = cmb.getProperty("id") as PropertyModelBuilder<BsonObjectId>
        c.codec(BsonObjectIdCodec())
        pojoCodecProvider.register(cmb.build())
    }
    return CodecRegistries.fromRegistries(
        MongoClientSettings.getDefaultCodecRegistry(),
        CodecRegistries.fromProviders(pojoCodecProvider.automatic(true).build()))
}