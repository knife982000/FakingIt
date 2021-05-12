package edu.isistan.fakenews

import edu.isistan.fakenews.crawler.TwitterCrawler
import edu.isistan.fakenews.crawler.TwitterStreamer
import edu.isistan.fakenews.storage.FAKE_NEWS_DB
import edu.isistan.fakenews.storage.MongoDBStorage
import edu.isistan.fakenews.webcrawler.OpenCVScreenshotCrawler
import edu.isistan.fakenews.webcrawler.ScreenshotCrawler
import edu.isistan.fakenews.webcrawler.SimpleScreenshotCrawler
import edu.isistan.fakenews.webcrawler.TwitterScreenshotCrawler
import edu.isistan.fakenews.webcrawler.WebHTMLCrawler
import edu.isistan.fakenews.scrapper.*

import org.apache.commons.cli.*
import org.slf4j.LoggerFactory
import java.io.File
import java.util.*
import kotlin.system.exitProcess
import edu.isistan.fakenews.webcrawler.TwitterObfuscatedScreenshotCrawler
import org.bson.types.ObjectId
import java.util.stream.Collectors
import org.apache.log4j.LogManager
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import edu.isistan.fakenews.webcrawler.TwitterScreenshotAndObfuscatedCrawler


private class FakeNewsCrawler

val LOGGER = LoggerFactory.getLogger(FakeNewsCrawler::class.java)


fun showHelp(options: Options) {
	val helpFormatter = HelpFormatter()
			helpFormatter.printHelp(
					"edu.isistan.fakenews.FakeNewsCrawlerKt",
					"Tool for querying and download tweets and their related URLs", options, "", true
					)
			exitProcess(0)
}

fun main(args: Array<String>) {

	//TODO:

	//Add search of the images in Google to see when have they been used and in which context -- check the pages for the keywords of the original search
	//Add ocr over images (not screenshots)

	//Add query augmentation
	//Add visualization

	//Add multi-thread support for tracking

	//Add option for getting news from specific sources


	//	val loggerContext = LoggerFactory.getILoggerFactory();
	//    val rootLogger = (loggerContext as LoggerContext).getLogger("org.mongodb.driver");
	//    rootLogger.setLevel(Level.INFO);	

	val options = Options()
			val groups = OptionGroup()
			groups.isRequired = true
			groups.addOption(Option("d", "download", false, "start the query download process"))
			groups.addOption(Option("w", "web", false, "start the Web content download process"))
			groups.addOption(
					Option(
							"s",
							"screenshot",
							false,
							"start the screenshot download process for all Tweets in the database or only those in the file"
							)
					)
			groups.addOption(
					Option(
							"sc",
							"scrapper",
							false,
							"determines whether to use the Twitter api or the scrapper when possible"
							)
					)

			val userRelations = Option(
					"ur",
					"user relations",
					false,
					"downloads the followee/follower relations of the already downloaded users. By default only downloads for users who wrote the core tweets. With -all downloads for every user in the database"
					)
			groups.addOption(userRelations)

			val userTweets = Option(
					"ut",
					"user tweets",
					false,
					"downloads the tweets of the already downloaded users. By default only downloads for users who wrote the core tweets. With -all downloads for every user in the database"
					)
			groups.addOption(userTweets)

			val usersTimeline = Option(
					"ua",
					"user timeline",
					true,
					"starts the user timeline download. It requires a list of userIds or screennames to download. The list can have both userIds or usernnames."
					)
			usersTimeline.argName = "file"
			usersTimeline.args = 1
			groups.addOption(usersTimeline)
	
			groups.addOption(
					Option(
							"u",
							"users",
							false,
							"downloads the info of missing users (e.g. those who have favourited or retweeted)"
							)
					)

			val add = Option("a", "add", true, "add queries for download. File is a text file with one query per line")
			add.argName = "file"
			add.args = 1
			groups.addOption(add)

			val tweet = Option("t", "tweets", true, "start the tweet download process")
			tweet.argName = "file"
			tweet.args = 1
			groups.addOption(tweet) //instead of using queries, you can download specific tweet ids

//			val replies = Option("treplies", "treplies", true, "start the reply download process")
//			tweet.argName = "file"
//			tweet.args = 1
//			groups.addOption(replies) /
//	
//			val quote = Option("tquotes", "tquotes", true, "start the quote download process")
//			tweet.argName = "file"
//			tweet.args = 1
//			groups.addOption(quote) 
	
			val tweetR = Option("tr", "inreply", true, "start the in reply to download process (only the tweets)")
			tweetR.argName = "file"
			tweetR.args = 1
			groups.addOption(tweetR)

			val tweetRF = Option(
					"trf",
					"inreplyfull",
					true,
					"start the full in reply to download process (the tweets + favorites and retweets)"
					)
			tweetRF.argName = "file"
			tweetRF.args = 1
			groups.addOption(tweetRF)

			val tweetScreenshots = Option(
					"st",
					"screenshot tweets",
					true,
					"start the twitter screenshot download process. Accepts a file with the tweets to use. Otherwise considers all tweets in the database"
					)
			tweetScreenshots.argName = "file"
			tweetScreenshots.args = 1
			groups.addOption(tweetScreenshots)

			groups.addOption(
					Option(
							"track",
							"track",
							false,
							"track topics, locations or users in real time. Configurations in property file"
							)
					)
			//may have option of whether to download replies and everything after the sampling?

			//special case of reconstructing a previously crawled dataset
			val search = Option(
					"search",
					"search",
					true,
					"search tweets by matching content. For each searched tweet, we only keep the closest"
					)
			search.argName = "file"
			search.args = 1
			groups.addOption(search) //textual search of tweets

			groups.addOption(Option("h", "help", false, "display this help and exit"))
			options.addOptionGroup(groups)

			options.addOption(
					Option(
							"conf",
							"configuration",
							true,
							"set the configuration property file, By default: settings.properties"
							)
					)

			options.addOption(
					Option(
							"rec",
							"recursive",
							false,
							"recursively download all replies chains (replies of replies). By default it does not download replies of replies"
							)
					)

			options.addOption(
					Option(
							"all",
							"all",
							false,
							"downloads user tweets or relations for all users in the database. By default, only for authors of core tweets"
							)
					)


			options.addOption(
					Option(
							"replies",
							"replies",
							false,
							"download all replies. By default it does not download tweet replies"
							)
					)
	
			options.addOption(
					Option(
							"quotes",
							"quotes",
							false,
							"download quotes to tweets. By default it does not download tweet quotes"
							)
					)

			val parser: CommandLineParser = DefaultParser()
			try { // parse the command line arguments
				val line = parser.parse(options, args) //TODO


						configure(line.getOptionValue("conf", "settings.properties")!!)
						when {
					line.hasOption("h") -> showHelp(options)
					line.hasOption("w") -> downloadWeb()
					line.hasOption("s") -> downloadScreenshot()
					line.hasOption("st") -> downloadTweetScreenshot(line.getOptionValue("st"))
					line.hasOption("a") -> addQueries(line.getOptionValue("a"), options)
					line.hasOption("track") -> trackRealTime(line.hasOption("rec"))
					line.hasOption("search") -> searchTweets(line.getOptionValue("search"), line.hasOption("rec"))

				}
				//								if(!line.hasOption("sc")){
				when {
					line.hasOption("d") -> download(line.hasOption("rec"))
					line.hasOption("t") -> downloadTweet(line.getOptionValue("t"), line.hasOption("rec"),line.hasOption("replies"),line.hasOption("quotes"))
					line.hasOption("tr") -> downloadInReplyTo(line.getOptionValue("tr"), false,line.hasOption("rec"))
					line.hasOption("trf") -> downloadInReplyTo(line.getOptionValue("trf"), true,line.hasOption("rec"))
//					line.hasOption("treplies") -> downloadReplies()
//					line.hasOption("tquotes") -> downloadQuotes()
					line.hasOption("u") -> downloadUsers()
					line.hasOption("ua") -> downloadUsersFull(line.getOptionValue("ua"))
					line.hasOption("ur") -> downloadUsersTweetsRelations(line.hasOption("all"),false,true,true)
					line.hasOption("ut") -> downloadUsersTweetsRelations(line.hasOption("all"),true,false,false)
				}
				//								}else{
				//									//TODO: Config the full scrapper
				//								}

			} catch (exp: ParseException) { // problem with the parameters, help is needed
				LOGGER.error("Parsing failed.  Reason: ${exp.message}")
				showHelp(options)
			}

}


fun configure(propertiesFile: String) {
	LOGGER.info("Loading settings from: {}", propertiesFile)
	val configurationFile = File(propertiesFile)
	if (!configurationFile.exists()) {
		LOGGER.error("Configuration file does not exist (${configurationFile.absoluteFile}). Consult help")
		exitProcess(0)
	}
	val properties = Properties()
			properties.load(configurationFile.inputStream())
			System.getProperties().putAll(properties)
}

fun downloadScreenshot() {
	LOGGER.info("Initializing DB")
	val storage = MongoDBStorage()
	val screenshotCrawler = SimpleScreenshotCrawler(storage)
	screenshotCrawler.run()
	screenshotCrawler.close()
}

fun downloadTweetScreenshot(filename: String?) {
	LOGGER.info("Initializing DB")
	val storage = MongoDBStorage()

	val tweetIds = ArrayList<Long>()
	if (filename == null) { //should download screenshots of all tweets in the database
		LOGGER.info("Downloading screenshots of every Tweet in database")
		tweetIds.addAll(storage.findTweets())
	} else {
		LOGGER.info("Downloading screenshots of selected Tweets")
		tweetIds.addAll(loadTweets(filename))
	}

	//	val screenshotCrawler = TwitterScreenshotCrawler(storage)
	//			screenshotCrawler.run(tweetIds)
	//			screenshotCrawler.close()
	//
	//			val obfuscatedScreenshotCrawler = TwitterObfuscatedScreenshotCrawler(storage)
	//			obfuscatedScreenshotCrawler.run(tweetIds)
	//			obfuscatedScreenshotCrawler.close()

	val normalAndObfuscatedScreenshotCrawler = TwitterScreenshotAndObfuscatedCrawler(storage)
			normalAndObfuscatedScreenshotCrawler.run(tweetIds)
			normalAndObfuscatedScreenshotCrawler.close()
}

fun downloadWeb() {
	LOGGER.info("Initializing DB")
	val storage = MongoDBStorage()
	val webCrawler = WebHTMLCrawler(storage)
	webCrawler.run()
}

@Suppress("NULLABILITY_MISMATCH_BASED_ON_JAVA_ANNOTATIONS")
fun addQueries(filename: String?, options: Options) {
	if (filename == null) {
		println("File name is undefined")
		showHelp(options)
	}
	val file = File(filename)
			if (!file.exists() || !file.isFile) {
				println("The specified root is not a file or does not exists")
				showHelp(options)
			}
	LOGGER.info("Initializing DB")
	val storage = MongoDBStorage()
	file.readLines().map { it.trim() }.filter { it.isNotEmpty() }.forEach {
		LOGGER.debug("Adding query: \"{}\"", it)
		storage.findOrStoreQueryDownload(it)
	}
	storage.close()
}

fun download(recursive: Boolean) {
	LOGGER.info("Initializing DB")
	val storage = MongoDBStorage()
	val twitterCrawler = TwitterCrawler(storage)
	twitterCrawler.run(recursive)
	storage.close()
}

private fun loadTweets(filename: String?): MutableList<Long> {

	val file = File(filename!!)
			if (!file.exists() || !file.isFile) {
				LOGGER.error("The specified root is not a file or does not exists {}", filename)
				return ArrayList<Long>()
			}

	val tweetIds = ArrayList<Long>(1_000_000)
			file.readLines().map { it.trim() }.filter { it.isNotEmpty() }.forEach {
				LOGGER.debug("Adding tweet: \"{}\"", it)
				tweetIds.add(it.toLong())

			}
			return tweetIds
}

fun downloadTweet(filename: String?, recursive: Boolean, replies : Boolean, quotes : Boolean) {

	if (filename == null) {
		LOGGER.error("File name is undefined")
		return
	}

	val tweetIds = loadTweets(filename)

			if (tweetIds.size != 0) {
				LOGGER.info("Initializing DB")
				val storage = MongoDBStorage()

				storage.findOrStoreQuery( //we store the query with the tweets to download
						filename,
						tweetIds
						)

				val tweetCrawler = TwitterCrawler(storage)
				tweetIds.chunked(2000).forEach{tweetCrawler.run(it, recursive, replies, quotes)}		
				storage.close()

				tweetCrawler.close()
				
			}

}

fun downloadUsers() {

	LOGGER.info("Initializing DB")
	val storage = MongoDBStorage()
	val tweetCrawler = TwitterCrawler(storage)

	val usersArray = storage.findUsersDownload().filter { it > 0} //To avoid problems with invalid users
	tweetCrawler.usersCrawl(usersArray.toLongArray(),false,false,false) //in case we are missing any user information from before!

	usersArray.filter { storage.findUser(it) != null }.forEach {
		storage.removeUserDownload(it) //we remove those users we effectively downloaded
	}

	val array = HashSet<Long>()
			storage.tweetFavorites.find().forEach {
		array.addAll(it.users)
	}
	tweetCrawler.usersCrawl(array.toLongArray(), false, false, false)

	array.clear()

	storage.tweetRetweeters.find().forEach {
		array.addAll(it.users)
	}
	tweetCrawler.usersCrawl(array.toLongArray(), false, false, false)

	storage.close()
}

fun downloadUsersTweetsRelations(all : Boolean = false,tweets: Boolean, followees: Boolean, followers: Boolean) { //for each user in users that it is not on followees/followers

	LOGGER.info("Initializing DB")
	val storage = MongoDBStorage()
	val tweetCrawler = TwitterCrawler(storage)

	if(all)
		tweetCrawler.usersCrawl(storage.findUsers().filter{it > 0}.toLongArray(), tweets, followees, followers)
		else //get tweets for queries, get users for those queries, call usersCrawl
			tweetCrawler.usersCrawl(storage.findAllQueryIds().asSequence().mapNotNull{it -> storage.findTweet(it)?.userId}.filter{it > 0}.toList().toLongArray(), tweets, followees, followers)

}

fun downloadUsersFull(filename : String?) {

	if (filename == null) {
		LOGGER.error("File name is undefined")
		return
	}
	val storage = MongoDBStorage()
	
	val userIds = mutableSetOf<Long>() //we need to check whether the given list has strings or longs
	val downloadUsers = mutableSetOf<String>()
	
	val file = File(filename)
	file.readLines().map { it.trim() }.filter { it.isNotEmpty() }.forEach {
				LOGGER.debug("Adding user: \"{}\"", it)
				try{
					userIds.add(it.toLong())

				}catch (e: NumberFormatException) {
//					val u = storage.findUser(it)
//					if (u != null)
//						userIds.add(u.userId)
//					else
						downloadUsers.add(it)
				}
	}
					
	val tweetCrawler = TwitterCrawler(storage)	
	tweetCrawler.usersCrawl(downloadUsers)
	downloadUsers.mapNotNull { it -> storage.findUser(it) }.forEach{u -> userIds.add(u.userId)}
		
	if(userIds.size == 0)
		return
	
	tweetCrawler.usersCrawl(userIds.toLongArray(), true, false, false)
	storage.close()
}

fun downloadInReplyTo(filename: String?, full: Boolean, recursive: Boolean) {
	if (filename == null) {
		LOGGER.error("File name is undefined")
		return
	}

	val tweetIds = loadTweets(filename)

			if (tweetIds.size != 0) {
				LOGGER.info("Initializing DB")
				val storage = MongoDBStorage()
				val tweetCrawler = TwitterCrawler(storage)
				tweetCrawler.downloadInReplyToTweets(tweetIds, full, recursive)
				storage.close()
			}

}

//I always assume there is going to be a limit to what we want to crawl... but what if the limit is too high?
fun trackRealTime(recursive: Boolean) {
	//first get parameters!
	val topics = System.getProperty("stream.topics", null)
			val language = System.getProperty("stream.language", null)
			val locations = System.getProperty("stream.locations", null)
			val users = System.getProperty("stream.users", null)
			val max_statuses = System.getProperty("stream.max_tweets", "20000")

			val storage = MongoDBStorage()
			val tweetStream = TwitterStreamer(storage)
			val tweetCrawler = TwitterCrawler(storage)


			var userIds: MutableSet<Long>? = null

			if (users != null) {

				userIds = mutableSetOf<Long>()
						val downloadUsers = mutableSetOf<String>()
						users.split(",").map { it.trim() }.forEach {
							try {
								userIds.add(it.toLong())
							} catch (e: NumberFormatException) {
								val u = storage.findUser(it)
										if (u != null)
											userIds.add(u.userId)
											else
												downloadUsers.add(it)
							}
						}
						//we need to download users by screnname as they were not included in the database --> TODO
						tweetCrawler.usersCrawl(downloadUsers)
						downloadUsers.forEach {
							val u = storage.findUser(it)
									if (u != null)
										userIds.add(u.userId)
						}

						//		tweetIds = tweetStream.trackUsers(userIds.toLongArray(),max_statuses.toInt())
			}
			//	else{ //topics, language, locations

			var topics_arr: Array<String>? = null
					var language_arr: Array<String>? = null
					var locations_arr: Array<DoubleArray>? = null

					if (topics != null)
						topics_arr = topics.split(",").toTypedArray()

						if (language != null)
							language_arr = language.split(",").toTypedArray()

							if (locations_arr != null) {
								val list = locations.split(",")
										if (list.size > 4)
											LOGGER.warn("The bounding box for location tracking is not well defined. Only the first four coordinates are considered.")
											locations_arr = arrayOf<DoubleArray>(
													doubleArrayOf(list[0].toDouble(), list[1].toDouble()),
													doubleArrayOf(list[2].toDouble(), list[3].toDouble())
													)
							}

			//		tweetIds = tweetStream.trackTopics(topics_arr,locations_arr,language_arr,max_statuses.toInt())

			//tweetIds = tweetStream.track(topics_arr,locations_arr,language_arr,userIds,max_statuses.toInt())
			val query = tweetStream.track(topics_arr, locations_arr, language_arr, userIds, max_statuses.toInt())

					//	}


					tweetCrawler.run(storage.findAllQueryIds(query).asSequence().toMutableList(), recursive)
					storage.close()
}

fun searchTweets(filename: String?, recursive: Boolean) {

	if (filename == null) {
		LOGGER.error("File name is undefined")
		return
	}

	val tweetTexts = mutableListOf<String>()

			val file = File(filename)
			file.readLines().map { it.trim() }.filter { it.isNotEmpty() }.forEach {
				tweetTexts.add(it)
			}

			if (tweetTexts.size == 0) {

				val tweetIds = searchTweets(tweetTexts)

						if (tweetIds.size > 0) {

							LOGGER.info("Initializing DB")
							val storage = MongoDBStorage()
							val tweetCrawler = TwitterCrawler(storage)
							tweetCrawler.run(tweetIds, recursive)
							storage.close()
						}
			}
}