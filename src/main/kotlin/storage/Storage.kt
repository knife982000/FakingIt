package edu.isistan.fakenews.storage

import java.util.*

/**
 * Represent the basic information of a tweet entity
 */
interface IEntity {
    var start: Int
    var end: Int
}

/**
 * Tweet entity containing an URL
 */
interface IURLEntity: IEntity{
    var url: String
    var expandedURL: String
    var displayURL: String
}

/**
 * GeoLocation dataclass
 */
data class GeoLocation(var latitude: Double=0.0, var longitude: Double=0.0)

/**
 * BaseEntity without extra information
 */
data class BaseEntity(override var start: Int=0, override var end: Int=0) : IEntity

/**
 * Entity representing a mention to an user in a tweet
 * @see Tweet
 */
data class UserEntity(var userId: Long=0,
                      override var start: Int=0,
                      override var end: Int=0): IEntity

/**
 * Entity representing a mention to an URL in a tweet
 * @see Tweet
 */
data class URLEntity(override var url: String="",
                     override var expandedURL: String="",
                     override var displayURL: String="",
                     override var start: Int=0,
                     override var end: Int=0) : IURLEntity

/**
 * Polygon or point representing the geographical location of a place
 * @see Place
 */
data class BoundingBox(var location: List<GeoLocation> = mutableListOf(),
                       var locationType: String = "")

/**
 * Entity representing a media URL
 */
data class MediaEntity(override var url: String="",
                       override var expandedURL: String="",
                       override var displayURL: String="",
                       override var start: Int=0,
                       override var end: Int=0,
                       var mediaId: Long=0,
                       var mediaURL: String="",
                       var mediaURLHttps: String="",
                       var type: String="",
                       var extAltText: String?=null) : IURLEntity


/**
 * Tweet information
 */
data class Tweet<T>(var id: T?=null,
                    var tweetId: Long=0,
                    var userId: Long=0, //Stored in another collection
//					var userScreenname : String?=null, //necesito guardarlo para los replies :( y por las dudas que los usuarios no estén todavía almacenados
                    var text: String="",
                    var created: Date= Date(),
                    var source: String="", //is it important?
                    var displayTextRangeStart: Int=0,
                    var displayTextRangeEnd: Int=0,
                    var truncated: Boolean=false,
                    var inReplyToStatusId: Long=-1,
                    var inReplyToUserId: Long=-1,
                    var inReplyToScreenName: String?=null,
                    var geoLocation: GeoLocation?=null,
                    var place: String?=null, //Store in another collection
                    var favoriteCount: Int=0,
                    var retweet: Boolean=false,
                    var retweetId: Long?=null,
                    var contributors: MutableList<Long>?=null,
                    var retweetCount: Int=0,
                    var PossiblySensitive: Boolean=false,
                    var lang: String?=null,
                    var quotedStatusId: Long=-1,
                    var scopes: MutableList<String>?=null,
                    var withheldInCountries: MutableList<String>?=null,
                    var userMentions: MutableList<UserEntity> = mutableListOf(),
                    var urlEntities: MutableList<URLEntity> = mutableListOf(),
                    var mediaEntity: MutableList<MediaEntity> = mutableListOf(),
                    var hastTags: MutableList<BaseEntity> = mutableListOf()
                    )

/**
 * Twitter User
 */
data class User<T>(var id: T?=null,
                   var userId: Long=0,
                   var name: String="",
                   var email: String?=null,
                   var screenName: String="",
                   var created: Date= Date(),
                   var location: String?=null,
                   var description: String?=null,
                   var contributorsEnabled: Boolean=false,
                   var profileImageURL: String="",
                   var biggerProfileImageURL: String?=null,
                   var miniProfileImageURL: String?=null,
                   var originalProfileImageURL: String?=null,
                   var profile400x400ImageURL: String?=null,
                   var profileImageURLHttps: String?=null,
                   var biggerProfileImageHttps: String?=null,
                   var miniProfileImageHttps: String?=null,
                   var originalProfileImageHttps: String?=null,
                   var profile400x400ImageHttps: String?=null,
                   var defaultProfileImage: Boolean=false,
                   var url: String?=null,
                   var protected: Boolean=false,
                   var followerCount: Int=0,
                   var status: Long?=null)

/**
 * Place
 */
data class Place<T>(var id: T?=null,
                    var placeId: String="",
                    var url: String="",
                    var placeType: String="",
                    var name: String="",
                    var fullName: String="",
                    var country: String="",
                    var boundingBox: BoundingBox,
                    var within: MutableList<String>?=null)

/**
 * Represents the result of a query
 */
data class Query<T>(var id: T?=null,
                    var text: String="",
                    var tweetIds: MutableList<Long> = mutableListOf())


data class UserTweets<T>(var id: T?=null,
                         var userId: Long=0,
                         var tweets: MutableList<Long> = mutableListOf())

data class TweetReplies<T>(var id: T?=null,
                         var tweetId: Long=0,
                         var replies: MutableList<Long> = mutableListOf())

data class TweetReactions<T>(var id: T?=null,
                         var tweetId: Long=0,
                         var users: MutableList<Long> = mutableListOf())

data class UserRelations<T>(var id: T?=null,
                            var userId: Long=0,
                            var bucket: Int=0,
                            var rel: MutableList<Long> = mutableListOf())
/**
 * Represents a query that has not been downloaded yet
 */
data class QueryDownload<T>(var id: T?=null,
                            var text: String="",
                            var maxId: Long = -1L)

/**
 * Represents a URL that has not been downloaded yet
 */
data class URLDownload<T>(var id: T?=null,
                          var url: String="",
                          var tweetId: Long = 0L)


/**
 * Represents an user whose followees and followers has not been downloaded yet
 */
data class UserDownload<T>(var id: T?=null,
                           var userId: Long = 0L)

data class CursorDownload<T>(var id: T?=null,
                             var userId: Long = 0L,
                             var cursor: Long = 0L,
                             var collection: String = "")