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


const val _USER = "#USER"
const val _TWEET = "#TWEET"
const val _POS = "#POS"
const val _WHAT = "retweeted"

const val replies_base_path = "https://twitter.com/${_USER}/status/${_TWEET}" 
const val replies_path = "https://twitter.com/i/${_USER}/conversation/${_TWEET}?include_available_features=1&include_entities=1&max_position=${_POS}&&reset_error_state=false"

const val reactions_base_path = "https://twitter.com/i/activity/${_WHAT}_popup?id=${_TWEET}"

//const val search_base_path = "https://twitter.com/i/search/timeline?f=tweets&vertical=default&include_available_features=1&include_entities=1&reset_error_state=false&src=typd&q=${_TWEET}&l=en"
//const val search_path = "https://twitter.com/i/searhttp://marketplace.eclipse.org/marketplace-client-intro?mpc_install=2257536ch/timeline?f=live&vertical=default&include_available_features=1&include_entities=1&reset_error_state=false&src=typd&max_position=${_POS}&q=${_TWEET}&l=en"
//const val search_path = "https://twitter.com/search?q=${_TWEET}&source=desktop-search"
//const val search_path = "https://twitter.com/i/search/timeline?f=live&src=typd&max_position=${_POS}&q=${_TWEET}&l=en"
//const val search_base_path = "https://mobile.twitter.com/search?q=${_TWEET}&s=typd&x=0&y=0"
const val search_path = "https://mobile.twitter.com/search?q=${_TWEET}&s=typd&next_cursor=${_POS}"

const val pattern_punct = "[\\p{Punct}+2]"
const val pattern_unico = "&#*[0-9a-z]*;"
const val pattern_RT = "RT @[a-zA-Z0-9]*:"
const val pattern_url = "((https?|ftp|gopher|telnet|file|Unsure|http):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*)";


class Scrapper 

var LOGGER = LoggerFactory.getLogger(Scrapper::class.java)!!


fun getReplies(screenname : String, tweetId : String, driver: WebDriver? = null) : List<Long> {

	val localDriver = when (driver) {
		null -> initFirefoxWithScrapperExtension()
		else -> driver
	}

	LOGGER.debug("Processing replies {} {}",screenname, tweetId)

	checkInternetAvailability()
	
	val replies = mutableListOf<Long>()
	val semaphore = Semaphore(0)
	//Server logic supplier
	val serverSupplier = Supplier<Jooby> {
		object :Jooby() {
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
	scrollConversation(screenname, tweetId, localDriver)
	semaphore.acquire()

	if (driver == null) {
		LOGGER.info("Closing Selenium Driver")
		localDriver.quit()
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
			}catch (e: Exception) {
				e.printStackTrace()
				exitProcess(1)
			}
}


private fun queryStaticHTLM(username : String, tweetid : String) : List<Any?> {

	val replies = ArrayList<Long>()
			var cursor : String? = null

			val url = replies_base_path.replace("#USER", username).replace("#TWEET",tweetid);

	LOGGER.debug("URL {}",url)

	var doc : Document? = null
	var retry = true;

	while(retry) {
		try{
			doc = Jsoup.connect(url).get();
			retry = false
		}catch(e : IOException){
			if(e.toString().contains("FileNotFound") || e.toString().contains("404") || e.toString().contains("403") || 
 e.message?.contains("FileNotFound")?:false || e.message?.contains("404")?:false || e.message?.contains("403")?:false){
				
				LOGGER.error("ERROR: {} {}",url,e.toString());
				
				retry = false
			}
			else{
				LOGGER.error("ERROR: {}",e.toString());
				checkInternetAvailability()
//				while(!checkInternetAvailability()){
//					LOGGER.error("ERROR: No internet ... waiting");
//					Thread.sleep(4000)
//				}

			}
			
		}
	}

	if(doc != null){
		doc.getElementsByClass("replies-to  permalink-inner permalink-replies").forEach{

			val e = it.getElementsByClass("tweets-wrapper").iterator().next().
					getElementsByClass("ThreadedDescendants").iterator().next().
					getElementsByClass("stream-container").iterator().next();
			cursor = e.attr("data-min-position");
		}

		replies.addAll(parseReplies(doc))
	}

	return listOf<Any?>(cursor,replies)
}

private fun parseReplies(doc : Document) : List<Long> {

	val reps = ArrayList<Long>()

			doc.getElementsByClass("stream-item-header")?.forEach{

		val line = it.getElementsByClass("tweet-timestamp js-permalink js-nav js-tooltip").attr("href");

		val sp = line.split("/");

		if(sp.size >= 4)
			reps.add(sp[3].replace("\"", "").toLong());	
	}
	return reps;
}

private fun getURLContent(url : String,enable_javascript : Boolean = true) : String?{

	LOGGER.debug("URL: {}",url)

	var con : HttpsURLConnection?
			var retry = true

			var content : String? = null
			
			while(retry) {
				try {
					con = URL(url).openConnection() as HttpsURLConnection;
					if(enable_javascript)
						con.setRequestProperty("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/603.3.8 (KHTML, like Gecko) Version/10.1.2 Safari/603.3.8");
					else
						con.setRequestProperty("User-Agent","ELinks/0.13.GIT (textmode; Linux 2.6.29 i686; 119x51-2)")
						val br =  BufferedReader(InputStreamReader(con.getInputStream()))

						content = br.lines().collect(Collectors.joining())

						try{
							val jsonObject = JsonParser().parse(content).getAsJsonObject()
									content = jsonObject.get("items_html").getAsString()
						}catch(e : Exception){}

					retry = false;

				} catch (e : IOException) {
					
								if(e.toString().contains("FileNotFound") || e.toString().contains("404") || e.toString().contains("403") || 
 e.message?.contains("FileNotFound")?:false || e.message?.contains("404")?:false || e.message?.contains("403")?:false){
				LOGGER.error("ERROR: Inexistent page {} {}",url,e.toString());
				retry = false
			}
			else{
				LOGGER.error("ERROR: {}",e.toString());
				checkInternetAvailability()
//				while(!checkInternetAvailability()){
//					LOGGER.error("ERROR: No internet ... waiting");
//					Thread.sleep(2000)
//				}

			}
							
			
				}
			}
	return content
}


fun getReactions(tweetId : String, what : String) : List<Long>{

	LOGGER.debug("Processing {} {} ",what, tweetId)

	val url = reactions_base_path.replace(_TWEET,tweetId).replace(_WHAT,what)

	LOGGER.debug("URL {}",url)

	//	var reactions : ArrayList<Long>? = null
	val reactions = ArrayList<Long>()

	var con : HttpsURLConnection?
			var retry = true
		
	while(retry) {
		try {
			con = URL(url).openConnection() as HttpsURLConnection;
			val br =  BufferedReader(InputStreamReader(con.getInputStream()))

					val jsonObject = JsonParser().parse(br.lines().collect(Collectors.joining())).getAsJsonObject()
					val html = jsonObject.get("htmlUsers").getAsString()

					val doc = Jsoup.parse(html);

			doc.getElementsByClass("account  js-actionable-user js-profile-popup-actionable ").forEach{
				reactions.add(it.attr("data-user-id").toLong())
			}

			retry = false;

		} catch (e : IOException) {

			if(e.toString().contains("FileNotFound") || e.toString().contains("404") || e.toString().contains("403") || 
 e.message?.contains("FileNotFound")?:false || e.message?.contains("404")?:false || e.message?.contains("403")?:false){
				LOGGER.error("ERROR: Inexistent tweet {} {}",tweetId,e.toString());
				retry = false
			}
			else{
				LOGGER.error("ERROR: {}",e.toString());
				checkInternetAvailability()
//				while(!checkInternetAvailability()){
//					LOGGER.error("ERROR: No internet ... waiting");
//					Thread.sleep(2000)
//				}

			}
		}
	}

	LOGGER.debug("Gotten reactions {} {} {}",tweetId,what,reactions.size)

	return reactions
}

private fun checkInternetAvailability() : Boolean{

	val url = "https://twitter.com"

	while(true){
			try{
				val con = URL(url).openConnection() as HttpsURLConnection;

				if(con.getResponseCode() == 200)
					return true
//							return false

			}catch(e : IOException){	
				LOGGER.error("Internet availability error {} waiting...",e.toString())
				Thread.sleep(4000)
//				return false
			}
	}
	
}

fun getSearchedTweet(text : String) : Long{

	var searchableText = "\"${text.replace(Regex(pattern_unico),"").replace(Regex(pattern_RT),"").replace(Regex(pattern_url),"").replace(Regex(pattern_punct),"").trim()}\""

			val tweets = mutableMapOf<Long,String>()

			var urlText : String? = search_path.replace(_TWEET,searchableText.replace(Regex("\\s"),"%20"))

			while(urlText != null) {

				val html = getURLContent(urlText,false)!!;

				val doc = Jsoup.parse(html)!!

						tweets.putAll(parseSearch(doc))

						val e = doc.getElementsByClass("w-button-more")
						if(e.size > 0)
							urlText = "https://mobile.twitter.com"+e.first().childNodes().get(1).attr("href")
							else
								urlText = null				
			}

			LOGGER.debug("Gotten search {} size: {}", text,tweets.size)

			if(tweets.size > 1)
				return getClosestTweet(searchableText,tweets)

						if(tweets.size == 1)
							return tweets.keys.iterator().next()

									return -1

											//	return closest	
}

private fun getClosestTweet(text : String, tweets : MutableMap<Long,String>) : Long {

	var closest = -1L
			var sim = Integer.MAX_VALUE
			var closest_text : String? = null

			tweets.forEach{

		val tt = it.value.replace(Regex(pattern_unico),"").replace(Regex(pattern_RT),"").replace(Regex(pattern_url),"").replace(Regex(pattern_punct),"")

				val d = editDistance(text,tt)
				//		println("$d ${it.value}")
				if(sim >= d){
					sim = d
							closest = it.key
							closest_text = it.value

				}
	}

	println("Closest tweet to \"${text}\" is \"${closest_text}\"")

	return closest
}

private fun parseSearch(doc : Document) : MutableMap<Long,String>{

	val tweets = mutableMapOf<Long,String>()

			doc.getAllElements().filter{it.className().startsWith("tweet-text")}.forEach{
				val id = it.attr("data-id").toLong()
						tweets.put(id,it.getElementsByClass("dir-ltr").text().toString())
			}

			return tweets
}

fun searchTweets(tweetTexts : MutableList<String>) : MutableList<Long>{
	val tweetIds = mutableListOf<Long>()

			tweetTexts.forEach{
		val closest = getSearchedTweet(it)
				if(closest > 0)
					tweetIds.add(closest)
	}

	return tweetIds
}


fun main(){
	
//	println(checkInternetAvailability())

	configure("settings.properties")
	
		val screenname = "OfeFernandez_"
	val tweet_id = "1210306953936855043"
	//	val tweet_id = "1247027648720756736"
		val list = getReplies(screenname,tweet_id)
	println(list)

//	var list = getReactions(tweet_id,"retweeted")
//	println(list);
//
//	list = getReactions(tweet_id,"favorited")
//			println(list);

	//	val text = "got ya bitch tip toeing on my hardwood floors \"\" &#128514; http://t.co/cOU2WQ5L4q\""
	//	val text = "loving you"

	//	val tweets = mutableListOf<String>()
	//	tweets.add("loving you")
	//	tweets.add("got ya bitch tip toeing on my hardwood floors")
	//	
	//	print(searchTweets(tweets))
}