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
data class BaseEntity(override var start: Int=0, override var end: Int) : IEntity

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
                    var userId: Long?=0, //Stored in another collection
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
                   var biggerProfileImageURL: String="",
                   var miniProfileImageURL: String="",
                   var originalProfileImageURL: String="",
                   var profile400x400ImageURL: String="",
                   var profileImageURLHttps: String="",
                   var biggerProfileImageHttps: String="",
                   var miniProfileImageHttps: String="",
                   var originalProfileImageHttps: String="",
                   var profile400x400ImageHttps: String="",
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

/**
 * Represents the social connection of an user
 */
data class UserConnections<T>(var id: T?,
                              var userId: Long=0,
                              var followees: MutableList<Long> = mutableListOf(),
                              var followers: MutableList<Long> = mutableListOf(),
                              var tweets: MutableList<Long> = mutableListOf())
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