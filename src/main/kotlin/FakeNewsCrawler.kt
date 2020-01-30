package edu.isistan.fakenews

import edu.isistan.fakenews.crawler.TwitterCrawler
import edu.isistan.fakenews.storage.DEBUG_DB
import edu.isistan.fakenews.storage.MongoDBStorage
import edu.isistan.fakenews.webcrawler.OpenCVScreenshotCrawler
import edu.isistan.fakenews.webcrawler.ScreenshotCrawler
import edu.isistan.fakenews.webcrawler.SimpleScreenshotCrawler
import edu.isistan.fakenews.webcrawler.TwitterScreenshotCrawler
import edu.isistan.fakenews.webcrawler.WebHTMLCrawler
import org.apache.commons.cli.*
import org.slf4j.LoggerFactory
import java.io.File
import java.util.*
import kotlin.system.exitProcess
import edu.isistan.fakenews.webcrawler.TwitterOfuscatedScreenshotCrawler
import org.bson.types.ObjectId
import java.util.stream.Collectors


private class FakeNewsCrawler

val LOGGER = LoggerFactory.getLogger(FakeNewsCrawler::class.java)


fun showHelp(options: Options) {
	val helpFormatter = HelpFormatter()
			helpFormatter.printHelp("edu.isistan.fakenews.FakeNewsCrawlerKt",
					"Tool for querying and download tweets and their related URLs", options,"", true)
			exitProcess(0)
}

fun main(args: Array<String>) {
	
	//TODO:
		
		//Add search tweets using scrapper --> para cuando tenemos solo el texto y queremos encontrar el tweet que matchea (medium to hard -- nothing done)
		
		//Add crawling de los tweets para arriba --> puede ser de una lista o de todos los que están ahí (easy to medium, has to search if it is retweet or quote,
			//if so, if it hasn't been download, call download tweets -- in replies maybe it is going to get heavy and repeated, call again this method until empty -- might be tedious)
		
		//Add search of the images in Google to see when have they been used and in which context -- check the pages for the keywords of the original search
		//Add ocr over images (not screenshots)
	
		//Add query augmentation
		//Add visualization
		
		//TODO: Las relaciones de usuarios que no se bajen al bajar los tweets! --> CAMBIAR!!
	
	val args1 = arrayOf("-st","C:\\Users\\Anto\\Desktop\\twitter-hate\\ids_to_download.txt")
	
	val options = Options()
			val groups = OptionGroup()
			groups.isRequired = true
			groups.addOption(Option("d", "download", false, "start the twitter download process"))
			groups.addOption(Option("w", "web", false, "start the Web content download process"))
			groups.addOption(Option("s", "screenshot", false, "start the screenshot download process for all Tweets in the database or only those in the file"))
			groups.addOption(Option("sc", "scrapper", false, "determines whether to use the Twitter api or the scrapper"))
			groups.addOption(Option("ur", "user relations", false, "downloads the followee/follower relations of the already downloaded users"))
			groups.addOption(Option("u", "users", false, "downloads the info of missing users (e.g. those who have favourited or retweeted)"))
			val add = Option("a", "add", true, "add queries for download. File is a text file with one query per line")
			add.argName = "file"
			add.args = 1
			groups.addOption(add)

			val tweet = Option("t", "tweets", true, "start the tweet download process")
			add.argName = "file"
			add.args = 1
			groups.addOption(tweet) //instead of using queries, you can download specific tweet ids

			val tweetScreenshots = Option("st", "screenshot tweets", true, "start the twitter screenshot download process. Accepts a file with the tweets to use. Otherwise considers all tweets in the database")
			add.argName = "file"
			add.args = 1
			groups.addOption(tweetScreenshots) 
	
			groups.addOption(Option("h", "help", false, "display this help and exit"))
			options.addOptionGroup(groups)

			options.addOption(Option("ddb", "fakeDB", false, "set the FakeNewsTest databese instead of the FakeNews (for testing purpose)"))
			options.addOption(Option("conf", "configuration", true, "set the configuration property file, By default: settings.properties"))

			val parser: CommandLineParser = DefaultParser()
			try { // parse the command line arguments
				val line = parser.parse(options, args1) 
						if (options.hasOption("ddb"))
							DEBUG_DB = true
							configure(line.getOptionValue("conf", "settings.properties")!!)
								when {
									line.hasOption("h") -> showHelp(options)
//									line.hasOption("d") -> download()
//									line.hasOption("t") -> downloadTweet(line.getOptionValue("t"))
									line.hasOption("w") -> downloadWeb()
									line.hasOption("s") -> downloadScreenshot()
									line.hasOption("st") -> downloadTweetScreenshot(line.getOptionValue("st"))
									line.hasOption("a") -> addQueries(line.getOptionValue("a"), options)		
								}
								if(!line.hasOption("sc")){
									when {
									line.hasOption("d") -> download()
									line.hasOption("t") -> downloadTweet(line.getOptionValue("t"))
									line.hasOption("u") -> downloadUsers()
									line.hasOption("ur") -> downloadUsersRelations()
									}
								}else{
									//TODO: Config the full scrapper
								}
								
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
	if (filename == null){ //should download screenshots of all tweets in the database
		LOGGER.info("Downloading screenshots of every Tweet in database")
		tweetIds.addAll(storage.findTweets())
	}
	else{
		LOGGER.info("Downloading screenshots of selected Tweets")
		tweetIds.addAll(loadTweets(filename)) 	
	}
		
	val screenshotCrawler = TwitterScreenshotCrawler(storage)
	screenshotCrawler.run(tweetIds)
	screenshotCrawler.close() 
	
	val obfuscatedScreenshotCrawler = TwitterOfuscatedScreenshotCrawler(storage)
	obfuscatedScreenshotCrawler.run(tweetIds)
	obfuscatedScreenshotCrawler.close()
}

fun downloadWeb() {
	LOGGER.info("Initializing DB")
	val storage = MongoDBStorage()
	val webCrawler = WebHTMLCrawler(storage)
	webCrawler.run()
}

@Suppress("NULLABILITY_MISMATCH_BASED_ON_JAVA_ANNOTATIONS")
fun addQueries(filename: String?, options: Options) {
	if (filename == null){
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
	file.readLines().map { it.trim() }.
	filter { it.isNotEmpty() }.
	forEach {
		LOGGER.debug("Adding query: \"{}\"", it)
		storage.findOrStoreQueryDownload(it)
	}
	storage.close()
}

fun download() {
	LOGGER.info("Initializing DB")
	val storage = MongoDBStorage()
	val twitterCrawler = TwitterCrawler(storage)
	twitterCrawler.run()
	storage.close()
}

private fun loadTweets(filename : String?) : MutableList<Long>{

	val file = File(filename)
			if (!file.exists() || !file.isFile) {
				LOGGER.error("The specified root is not a file or does not exists {}",filename)
				return ArrayList<Long>()
			}
	
	val tweetIds = ArrayList<Long>()
	file.readLines().map { it.trim() }.
	filter { it.isNotEmpty() }.
	forEach {
		LOGGER.debug("Adding tweet: \"{}\"", it)
		tweetIds.add(it.toLong())
		
	}
	return tweetIds
}

fun downloadTweet(filename: String?) {
	
	if (filename == null){
		LOGGER.error("File name is undefined")
		return
	}
		
	val tweetIds = loadTweets(filename)
	
	if(tweetIds.size != 0){
		LOGGER.info("Initializing DB")
		val storage = MongoDBStorage()
		val tweetCrawler = TwitterCrawler(storage)
		tweetCrawler.run(tweetIds)
		storage.close()
	}
		
}

fun downloadUsers() {
	
	LOGGER.info("Initializing DB")
	val storage = MongoDBStorage()
	val tweetCrawler = TwitterCrawler(storage)
	
	val usersArray = storage.findUsersDownload()
	tweetCrawler.usersCrawl(usersArray.toLongArray(),false,false,false) //in case we are missing any user information from before!
	usersArray.filter{ storage.findUser(it) != null}.forEach{
		storage.removeUserDownload(it) //we remove those users we effectively downloaded
	}
	
	val array = HashSet<Long>()
	storage.tweetFavorites.find().forEach{
		array.addAll(it.users)
	}
	tweetCrawler.usersCrawl(array.toLongArray(),false,false,false)
	
	array.clear()
	
	storage.tweetRetweeters.find().forEach{
		array.addAll(it.users)
	}
	tweetCrawler.usersCrawl(array.toLongArray(),false,false,false)
	
	storage.close()
}

fun downloadUsersRelations(){ //for each user in users that it is not on followees/followers
	
	LOGGER.info("Initializing DB")
	val storage = MongoDBStorage()
	val tweetCrawler = TwitterCrawler(storage)

	tweetCrawler.usersCrawl(storage.findUsers().toLongArray(),false,true,true)
}
