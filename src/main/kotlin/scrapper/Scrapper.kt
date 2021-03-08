package edu.isistan.fakenews.scrapper

import com.google.gson.Gson
import org.jsoup.Jsoup
import org.jsoup.nodes.Document

import com.google.gson.JsonObject
import com.google.gson.JsonParser
import org.slf4j.LoggerFactory

import javax.net.ssl.HttpsURLConnection
import java.net.URL
import java.util.stream.Collectors

import edu.isistan.fakenews.webcrawler.editDistance
import org.jooby.Jooby
import org.jooby.Request
import org.jooby.Route
import org.openqa.selenium.*
import org.openqa.selenium.support.ui.WebDriverWait
import java.io.*
import java.lang.Exception
import java.util.concurrent.Semaphore
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Supplier
import kotlin.system.exitProcess
import com.google.gson.stream.MalformedJsonException
import org.jsoup.select.Elements
import java.net.HttpURLConnection

import edu.isistan.fakenews.*
import edu.isistan.fakenews.storage.Tweet
import org.bson.types.ObjectId
import java.net.URLEncoder


const val _USER = "#USER"
const val _TWEET = "#TWEET"
const val _POS = "#POS"
const val _WHAT = "retweeted"

var bearer : String? = null
var guest_token : String? = null

val user_agent = "Mozilla/5.0 (X11; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/96.0"

//const val search_base_path = "https://twitter.com/i/search/timeline?f=tweets&vertical=default&include_available_features=1&include_entities=1&reset_error_state=false&src=typd&q=${_TWEET}&l=en"
//const val search_path = "https://twitter.com/i/searhttp://marketplace.eclipse.org/marketplace-client-intro?mpc_install=2257536ch/timeline?f=live&vertical=default&include_available_features=1&include_entities=1&reset_error_state=false&src=typd&max_position=${_POS}&q=${_TWEET}&l=en"
//const val search_path = "https://twitter.com/search?q=${_TWEET}&source=desktop-search"
//const val search_path = "https://twitter.com/i/search/timeline?f=live&src=typd&max_position=${_POS}&q=${_TWEET}&l=en"
//const val search_base_path = "https://mobile.twitter.com/search?q=${_TWEET}&s=typd&x=0&y=0"
const val search_path = "https://mobile.twitter.com/search?q=${_TWEET}&s=typd&next_cursor=${_POS}"

const val pattern_punct = "[\\p{Punct}+2]"
const val pattern_unico = "&#*[0-9a-z]*;"
const val pattern_RT = "RT @[a-zA-Z0-9]*:"
const val pattern_url =
    "((https?|ftp|gopher|telnet|file|Unsure|http):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*)";

class Scrapper

var LOGGER = LoggerFactory.getLogger(Scrapper::class.java)!!

fun getUserTweets(screenname: String, since: String?, until: String?, query: String = "", driver: WebDriver? = null): Set<Long> {

    var localDriver: WebDriver? = when (driver) {
        null -> initFirefoxWithScrapperExtension()
        else -> driver
    }

    LOGGER.debug("Processing tweets {} {} {} {}", screenname, since, until, query)

    checkInternetAvailability()

    val replies = mutableListOf<Long>()
    val semaphore = Semaphore(0)
    //Server logic supplier
    val serverSupplier = Supplier<Jooby> {
        object : Jooby() {
            val mutex = ReentrantLock()
            val gson = Gson()
            var exitOk = false

            init {
                checkInternetAvailability()
                post("/search", Route.OneArgHandler { this.addSearch(it) })
                post("/error", Route.OneArgHandler {
                    LOGGER.warn("Error in the Extension ${it.body().value()}. If data is empty it might be ignored.")
                })
                get("/quit", Route.ZeroArgHandler { this.close() })
            }

            fun addSearch(request: Request): String {
                val req = request.body().value()

                val map = gson.fromJson(req, Map::class.java)
                //val tweets = ((map["tweets"] as Map<String, Any?>).keys.toList()).map { it.toLong() }
                val tweetsUsers = (map["tweets"] as Map<String, Map<String, String>>).values.map {
                    it["id_str"] to it["user_id_str"]
                }
                val usersId = (map["users"] as Map<String, Map<String, String>>).values.map {
                    it["id_str"] to it["screen_name"]
                }.find {
                    screenname == it.second
                }?.first
                val tweets = usersId?.let {
                    val id = it
                    tweetsUsers.filter {
                        it.second == id && it.first != null
                    }.map { it.first!!.toLong() }
                } ?: emptyList()
                mutex.lock()
                replies.addAll(tweets)
                mutex.unlock()
                return "Ok"
            }

            //Closes the server and returns the control to the main thread
            fun close(): String {
                this.exitOk = true
                Thread {
                    this.stop()
                    semaphore.release()
                }.start()
                return "Ok"
            }
        }
    }
    //Starting server
    Jooby.run(serverSupplier, "application.port=8080")
    //Scroll twitter with extension
    scrollSearch(screenname, since, until, query, localDriver!!)
    semaphore.acquire()

    if (driver == null) {
        LOGGER.info("Closing Selenium Driver")
        localDriver.quit()
    }
    return replies.toSet();
}

fun scrollSearch(screenname: String, since: String?, until: String?, query: String, driver: WebDriver) {
	
	var url = "https://twitter.com/search?q=from%3A${screenname}%20${query}"
	if(since != null)
		url = url+"%20since%3A${since}"
	if(until != null)
		url = url+"%20until%3A${until}"

    println(url)

    driver.get(url)
    driver.manage().window().maximize()
    WebDriverWait(driver, 10)
    val javascript = driver as JavascriptExecutor
    val scroll = 348//driver.manage().window().size.height / 2

    var lastTop = -1
    Thread.sleep(5000)//Wait to load... it is horrible but it kind of work!

    try {
        while (lastTop != windowScrollY(javascript).toInt()) {
            javascript.executeScript(
                """
							if (document.getElementsByClassName("css-18t94o4 css-1dbjc4n r-1777fci r-1jayybb r-o7ynqc r-1j63xyz r-13qz1uu").length == 1) {
								document.getElementsByClassName("css-18t94o4 css-1dbjc4n r-1777fci r-1jayybb r-o7ynqc r-1j63xyz r-13qz1uu")[0].click()
							}
							""".trimIndent()
            )
            Thread.sleep(1000)//Wait to load... it is horrible but it kind of work!
            lastTop = windowScrollY(javascript).toInt()
            javascript.executeScript("window.scrollTo(0, ${lastTop + scroll})")
        }
        //closes the web service
        //and the driver
        javascript.executeScript(
            """
						let xhttp = new XMLHttpRequest()
						xhttp.open("GET", "http://localhost:8080/quit") 
						xhttp.send()
						""".trimIndent()
        )
    } catch (e: Exception) {
        e.printStackTrace()
        exitProcess(1)
    }
}

fun getReplies(screenname: String, tweetId: String, driver: WebDriver? = null): MutableList<Long> {

    var localDriver: WebDriver? = when (driver) {
        null -> initFirefoxWithScrapperExtension()
        else -> driver
    }

    LOGGER.debug("Processing replies {} {}", screenname, tweetId)

    checkInternetAvailability()

    val replies = mutableListOf<Long>()
    val semaphore = Semaphore(0)
    //Server logic supplier
    val serverSupplier = Supplier<Jooby> {
        object : Jooby() {
            val mutex = ReentrantLock()
            val gson = Gson()
            var exitOk = false

            init {
                checkInternetAvailability()
                post("/replies", Route.OneArgHandler { this.addReplies(it) })
                post("/error", Route.OneArgHandler {
                    LOGGER.warn("Error in the Extension ${it.body().value()}. If data is empty it might be ignored.")
                })
                get("/quit", Route.ZeroArgHandler { this.close() })
            }

            fun addReplies(request: Request): String {

                val map = gson.fromJson(request.body().value(), Map::class.java)
                val tweets = (map.keys.toList() as List<String>).map { it.toLong() }
                mutex.lock()
                replies.addAll(tweets)
                mutex.unlock()
                return "Ok"
            }

            //Closes the server and returns the control to the main thread
            fun close(): String {
                this.exitOk = true
                Thread {
                    this.stop()
                    semaphore.release()
                }.start()
                return "Ok"
            }
        }
    }
//Starting server
    Jooby.run(serverSupplier, "application.port=8080")
//Scroll twitter with extension
    scrollConversation(screenname, tweetId, localDriver!!)
    semaphore.acquire()

    if (driver == null) {
        LOGGER.info("Closing Selenium Driver")
        localDriver.quit()
        localDriver = null
    }
    replies.removeAll { it == tweetId.toLong() }
    return replies;
}

private fun scrollConversation(screenname: String, tweetId: String, driver: WebDriver) {
    val url = "https://twitter.com/${screenname}/status/${tweetId}"
    driver.get(url)
    driver.manage().window().maximize()
    WebDriverWait(driver, 10)
    val javascript = driver as JavascriptExecutor
    val scroll = 348//driver.manage().window().size.height / 2

    var lastTop = -1
    Thread.sleep(5000)//Wait to load... it is horrible but it kind of work!

    try {
        while (lastTop != windowScrollY(javascript).toInt()) {
            javascript.executeScript(
                """
							if (document.getElementsByClassName("css-18t94o4 css-1dbjc4n r-1777fci r-1jayybb r-o7ynqc r-1j63xyz r-13qz1uu").length == 1) {
								document.getElementsByClassName("css-18t94o4 css-1dbjc4n r-1777fci r-1jayybb r-o7ynqc r-1j63xyz r-13qz1uu")[0].click()
							}
							""".trimIndent()
            )
            Thread.sleep(1000)//Wait to load... it is horrible but it kind of work!
            lastTop = windowScrollY(javascript).toInt()
            javascript.executeScript("window.scrollTo(0, ${lastTop + scroll})")
        }
        //closes the web service
        //and the driver
        javascript.executeScript(
            """
						let xhttp = new XMLHttpRequest()
						xhttp.open("GET", "http://localhost:8080/quit") 
						xhttp.send()
						""".trimIndent()
        )
    } catch (e: Exception) {
        e.printStackTrace()
        exitProcess(1)
    }
}

private fun getCredentials(user_agent: String) : Pair<String?,String?>{
	
	var url = "https://abs.twimg.com/responsive-web/client-web/main.90f9e505.js"
	
    var con: HttpsURLConnection?

    var content: String?

    con = URL(url).openConnection() as HttpsURLConnection
 
    con.setRequestProperty("User-Agent", user_agent);
																																																  
    var br = BufferedReader(InputStreamReader(con.getInputStream()))
    content = br.lines().collect(Collectors.joining())

	val regex = "s=\"AAAAA[^\"]+\"".toRegex()
		
	val matches = regex.find(content)
	
	var bearer : String? = null
	
	if(matches != null)
		bearer = matches.groupValues.get(0).substring(3).dropLast(1)
	
	if(bearer == null)
		return Pair<String?,String?>(null,null)
	
	url = "https://api.twitter.com/1.1/guest/activate.json"
    con = URL(url).openConnection() as HttpsURLConnection
    con.requestMethod = "POST"
	
	con.setRequestProperty("User-Agent", user_agent);
    con.setRequestProperty("Content-Type", "application/json")
	con.setRequestProperty("Authorization", "Bearer $bearer")
	
	br = BufferedReader(InputStreamReader(con.getInputStream()))
    content = br.lines().collect(Collectors.joining())

	val guest_token = JsonParser().parse(content).getAsJsonObject().get("guest_token").getAsString()
		
	return Pair<String,String>(bearer,guest_token)
	
}

// we could add a counter for those cases in which a 429 occurs and we want to retry in several minutes
fun getPage(url : String, user_agent : String, tweetId : String) : Triple<List<Long>,String?,Long>{
	
	var con: HttpsURLConnection?
	var br : BufferedReader?
	var content : String? = null
	
	try {
		con = URL(url).openConnection() as HttpsURLConnection
		
		con.setRequestProperty("User-Agent", user_agent)
	    con.setRequestProperty("Content-Type", "application/json")
		con.setRequestProperty("Authorization", "Bearer $bearer")
		con.setRequestProperty("x-guest-token", guest_token)
		
		br = BufferedReader(InputStreamReader(con.getInputStream()))
	    content = br.lines().collect(Collectors.joining())
	
	}
	catch(e : IOException){
		LOGGER.error("Exception in scrapping replies: ${e.message}")
				val pair = getCredentials(user_agent)
				bearer = pair.first
				guest_token = pair.second
	}
		
	if(content == null)
		return Triple<List<Long>,String?,Long>(mutableListOf<Long>(),null,-1)
	
	val objects = JsonParser().parse(content).getAsJsonObject()
	
	val tweets_json = objects.get("globalObjects").getAsJsonObject().get("tweets").getAsJsonObject()
		
	val replies = tweets_json.entrySet().map{it.key.toLong()}.toList()
	
	var cant_replies = 0L
	val searched = tweets_json.get(tweetId)
	if(searched != null)
		cant_replies = tweets_json.get(tweetId).getAsJsonObject().get("reply_count").getAsLong()
	
	if(replies.size <= 1)
		Pair<List<Long>,String?>(mutableListOf<Long>(),null)

	val entries = objects.get("timeline").getAsJsonObject().get("instructions").getAsJsonArray()[0]
						 .getAsJsonObject()?.get("addEntries")?.getAsJsonObject()?.get("entries")?.getAsJsonArray()
	
//	val entries = objects.get("timeline").getAsJsonObject().get("instructions").getAsJsonArray().map{it}
//						 .mapNotNull{it.getAsJsonObject().get("addEntries")}
//						 .mapNotNull{it.getAsJsonObject().get("entries")}.mapNotNull{it.getAsJsonArray()}.flatMap{it}
//
	if(entries == null)
		return Triple<List<Long>,String?,Long>(replies,null,cant_replies)
	
	val cursor = entries.mapNotNull{it.getAsJsonObject().get("content")}
		   .mapNotNull{it.getAsJsonObject().get("operation")}
		   .mapNotNull{it.getAsJsonObject().get("cursor")}
		   .mapNotNull{it.getAsJsonObject().get("value")}
		   .map{it.getAsString()}
		   .toList().getOrNull(0)
	
//	val cursor = entries.mapNotNull{it.getAsJsonObject().get("content")}
//		   .mapNotNull{it.getAsJsonObject().get("timelineModule")}
//		   .mapNotNull{it.getAsJsonObject().get("items")}
//		   .mapNotNull{it.getAsJsonArray()}.flatMap{it}
//		   .mapNotNull{it.getAsJsonObject().get("item")}
//		   .mapNotNull{it.getAsJsonObject().get("content")}
//		   .mapNotNull{it.getAsJsonObject().get("timelineCursor")}
//		   .mapNotNull{it.getAsJsonObject().get("value")}
//		   .map{it.getAsString()}
//		   .toList().getOrNull(0)
	
	
	return Triple<List<Long>,String?,Long>(replies,cursor,cant_replies)
}

fun getScrapedReplies(screenname: String, tweetId: String): MutableList<Long> {

    LOGGER.debug("Scrapping replies {} {}", screenname, tweetId)

	if(bearer == null || guest_token == null){
		val pair = getCredentials(user_agent)
		bearer = pair.first
		guest_token = pair.second
	}
	 
	if(bearer == null || guest_token == null)
		return mutableListOf<Long>(-1)
	
	val all_replies = mutableSetOf<Long>()
	
	var (replies,cursor,cant_replies) = getPage("https://twitter.com/i/api/2/timeline/conversation/$tweetId.json?include_reply_count=1",user_agent,tweetId)
		
	all_replies.addAll(replies)
	
	while(cursor != null){
		LOGGER.debug("Scrapping replies cursor {} {}", screenname, tweetId)
		val url = "https://twitter.com/i/api/2/timeline/conversation/$tweetId.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweet=true&referrer=tweet&count=20&cursor=${URLEncoder.encode(cursor, "utf-8")}&include_ext_has_birdwatch_notes=false&ext=mediaStats%2ChighlightedLabel"
		val triple = getPage(url,user_agent,tweetId)
		cursor = triple.second
		all_replies.addAll(triple.first)
	}
	
	all_replies.remove(tweetId.toLong())
	LOGGER.debug("Obtained replies {} {}", cant_replies, replies)
	
	if(cant_replies == -1L || (cant_replies > 10 && cant_replies > all_replies.size))
		return mutableListOf<Long>(-1)

    return all_replies.toMutableList();
}

// the idea here is to avoid making useless request to the api, we can remove all tweets with zero retweets
fun getRetweetCount(tweetId : String) : Long{
	
	LOGGER.debug("Getting retweet count {}", tweetId)
	
	if(bearer == null || guest_token == null){
		val pair = getCredentials(user_agent)
		bearer = pair.first
		guest_token = pair.second
	}
	
	if(bearer == null || guest_token == null)
		return -1
	
	var con: HttpsURLConnection?
	var br : BufferedReader?
	var content : String? = null
	
	try {
		con = URL("https://twitter.com/i/api/2/timeline/conversation/$tweetId.json").openConnection() as HttpsURLConnection
		
		con.setRequestProperty("User-Agent", user_agent)
	    con.setRequestProperty("Content-Type", "application/json")
		con.setRequestProperty("Authorization", "Bearer $bearer")
		con.setRequestProperty("x-guest-token", guest_token)
		
		br = BufferedReader(InputStreamReader(con.getInputStream()))
	    content = br.lines().collect(Collectors.joining())
	}
	catch(e : IOException){
		LOGGER.error("Exception in getting retweet count: ${e.message}")
	}
		
	if(content == null)
		return -1
	
	val objects = JsonParser().parse(content).getAsJsonObject()
	
	val tweets_json = objects.get("globalObjects").getAsJsonObject().get("tweets").getAsJsonObject()
			
	var cant_retweets = -1L
	val searched = tweets_json.get(tweetId)
	if(searched != null)
		cant_retweets = tweets_json.get(tweetId).getAsJsonObject().get("retweet_count").getAsLong()
	
	return cant_retweets
	
}

private fun getURLContent(url: String, enable_javascript: Boolean = true): String? {

    LOGGER.debug("URL: {}", url)

    var con: HttpsURLConnection?
    var retry = true

    var content: String? = null

    while (retry) {
        try {
            con = URL(url).openConnection() as HttpsURLConnection;
            if (enable_javascript)
                con.setRequestProperty(
                    "User-Agent",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/603.3.8 (KHTML, like Gecko) Version/10.1.2 Safari/603.3.8"
                );
																																																   
	 
            else
                con.setRequestProperty("User-Agent", "ELinks/0.13.GIT (textmode; Linux 2.6.29 i686; 119x51-2)")

//					con.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:60.0) Gecko/20100101 Firefox/60.0")
            val br = BufferedReader(InputStreamReader(con.getInputStream()))

            content = br.lines().collect(Collectors.joining())

            try {
                val jsonObject = JsonParser().parse(content).getAsJsonObject()
                content = jsonObject.get("items_html").getAsString()
            } catch (e: Exception) {
            }

            retry = false;

        } catch (e: IOException) {

            if (e.toString().contains("FileNotFound") || e.toString().contains("404") || e.toString().contains("403") ||
                e.message?.contains("FileNotFound") ?: false || e.message?.contains("404") ?: false || e.message?.contains(
                    "403"
                ) ?: false
            ) {
                LOGGER.error("ERROR: Inexistent page {} {}", url, e.toString());
                retry = false
            } else {
		  
                LOGGER.error("ERROR: {}", e.toString());
                checkInternetAvailability()

            }


        }
    }
    return content
}



private fun checkInternetAvailability(): Boolean {

    val url = "https://twitter.com"

    while (true) {
        try {
            val con = URL(url).openConnection() as HttpsURLConnection
            //Mask the connection as a Chrome 86.0.4240 connection
            con.setRequestProperty(
                "User-Agent",
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) " +
                        "Chrome/86.0.4240.183 Safari/537.36 " +
                        "RuxitSynthetic/1.0 v8239450947 t38550 ath9b965f92 " +
                        "altpub cvcv=2"
            )

            if (con.getResponseCode() == 200)
                return true
            //							return false

        } catch (e: IOException) {
            LOGGER.error("Internet availability error {} waiting...", e.toString())
            Thread.sleep(4000)
            //				return false
        }
    }

}

fun getSearchedTweet(text: String): Long {

    var searchableText = "\"${
        text.replace(Regex(pattern_unico), "").replace(Regex(pattern_RT), "").replace(Regex(pattern_url), "")
            .replace(Regex(pattern_punct), "").trim()
    }\""

    val tweets = mutableMapOf<Long, String>()

    var urlText: String? = search_path.replace(_TWEET, searchableText.replace(Regex("\\s"), "%20"))

    while (urlText != null) {

        val html = getURLContent(urlText, false)!!;

        val doc = Jsoup.parse(html)!!

        tweets.putAll(parseSearch(doc))

        val e = doc.getElementsByClass("w-button-more")
        if (e.size > 0)
            urlText = "https://mobile.twitter.com" + e.first().childNodes().get(1).attr("href")
        else
            urlText = null
    }

    LOGGER.debug("Gotten search {} size: {}", text, tweets.size)

    if (tweets.size > 1)
        return getClosestTweet(searchableText, tweets)

    if (tweets.size == 1)
        return tweets.keys.iterator().next()

    return -1

    //	return closest
}

private fun getClosestTweet(text: String, tweets: MutableMap<Long, String>): Long {

    var closest = -1L
    var sim = Integer.MAX_VALUE
    var closest_text: String? = null

    tweets.forEach {

        val tt =
            it.value.replace(Regex(pattern_unico), "").replace(Regex(pattern_RT), "").replace(Regex(pattern_url), "")
                .replace(Regex(pattern_punct), "")

        val d = editDistance(text, tt)
        //		println("$d ${it.value}")
        if (sim >= d) {
            sim = d
            closest = it.key
            closest_text = it.value

        }
    }

    println("Closest tweet to \"${text}\" is \"${closest_text}\"")

    return closest
}

private fun parseSearch(doc: Document): MutableMap<Long, String> {

    val tweets = mutableMapOf<Long, String>()

    doc.getAllElements().filter { it.className().startsWith("tweet-text") }.forEach {
        val id = it.attr("data-id").toLong()
        tweets.put(id, it.getElementsByClass("dir-ltr").text().toString())
    }

    return tweets
}

fun searchTweets(tweetTexts: MutableList<String>): MutableList<Long> {
    val tweetIds = mutableListOf<Long>()

    tweetTexts.forEach {
        val closest = getSearchedTweet(it)
        if (closest > 0)
            tweetIds.add(closest)
    }

    return tweetIds
}

fun getSearchedPotentialReplies(text: String, tweetId: Long, since: String, until: String): List<Long> {
    val tweets = mutableMapOf<Long, String>()
    var urlText: String? = "https://mobile.twitter.com/search?q=(to:$text)(since:$since)(until:$until)"
    while (urlText != null) {
        val html = getURLContent(urlText, false)!!;
        println(html)
        val doc = Jsoup.parse(html)!!
        tweets.putAll(parseSearch(doc))
        val e = doc.getElementsByClass("w-button-more")
        urlText = if (e.size > 0)
            "https://mobile.twitter.com" + e.first().childNodes().get(1).attr("href")
        else {
            null
        }
    }
    LOGGER.debug("Gotten search {} size: {}", text, tweets.size)
    return tweets.keys.filter { it > tweetId }.toList()
}
