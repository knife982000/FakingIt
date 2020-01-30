package edu.isistan.fakenews.scrapper

import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.jsoup.nodes.Element

import com.google.gson.JsonArray
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import java.io.IOException
import org.slf4j.LoggerFactory

import javax.net.ssl.HttpsURLConnection
import java.net.URL
import java.io.InputStreamReader
import java.io.BufferedReader
import java.util.stream.Collectors

import edu.isistan.fakenews.webcrawler.editDistance

class Scrapper 

const val _USER = "#USER"
const val _TWEET = "#TWEET"
const val _POS = "#POS"
const val _WHAT = "retweeted"

const val replies_base_path = "https://twitter.com/${_USER}/status/${_TWEET}" 
const val replies_path = "https://twitter.com/i/${_USER}/conversation/${_TWEET}?include_available_features=1&include_entities=1&max_position=${_POS}&&reset_error_state=false"

const val reactions_base_path = "https://twitter.com/i/activity/${_WHAT}_popup?id=${_TWEET}"

const val search_base_path = "https://twitter.com/i/search/timeline?f=tweets&vertical=default&include_available_features=1&include_entities=1&reset_error_state=false&src=typd&q=${_TWEET}&l=en"

const val search_path = "https://twitter.com/i/search/timeline?f=live&vertical=default&include_available_features=1&include_entities=1&reset_error_state=false&src=typd&max_position=${_POS}&q=${_TWEET}&l=en"
//const val search_path = "https://twitter.com/search?q=${_TWEET}&source=desktop-search"
//const val search_path = "https://twitter.com/i/search/timeline?f=live&src=typd&max_position=${_POS}&q=${_TWEET}&l=en"
var LOGGER = LoggerFactory.getLogger(Scrapper::class.java)!!


fun getReplies(screenname : String, tweetId : String) : List<Long> {

	LOGGER.debug("Processing replies {} {}",screenname, tweetId)

	val replies = ArrayList<Long>()

	var things = queryStaticHTLM(screenname,tweetId)
	var cursor = things.get(0) as String?

	val urlReplies = replies_path.replace(_TWEET,tweetId).replace(_USER,screenname)
	
	replies.addAll(things.get(1) as List<Long>)

			while(cursor != null) {

				val url = urlReplies.replace(_POS,cursor)
				val jsonObject = getURLContent(url)!!;
				
				println(jsonObject)
				
				val html = jsonObject.get("items_html").getAsString()

				val doc = Jsoup.parse(html);

				replies.addAll(parseReplies(doc))
				
				if(jsonObject.get("has_more_items").getAsBoolean())
					cursor = jsonObject.get("min_position").getAsString();
				else 
					cursor = doc.getElementsByClass("ThreadedConversation-showMoreThreadsButton u-textUserColor")?.first()?.attr("data-cursor")
				
			}

	LOGGER.debug("Gotten replies {} {} size: {}",screenname, tweetId,replies.size)

	return replies;
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
		}catch(e1 : IOException){
			if(e1.message!!.contains("404") || e1.message!!.toLowerCase().contains("not found"))
				retry = false;
			LOGGER.error("Retry!! {}",e1.message);
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

private fun getURLContent(url : String) : JsonObject?{

	LOGGER.debug("URL: {}",url)

	var con : HttpsURLConnection?
	var retry = true

	var jsonObject : JsonObject? = null
	
		while(retry) {
			try {
				con = URL(url).openConnection() as HttpsURLConnection;
				con.setRequestProperty("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/603.3.8 (KHTML, like Gecko) Version/10.1.2 Safari/603.3.8");
				val br =  BufferedReader(InputStreamReader(con.getInputStream()))

				jsonObject = JsonParser().parse(br.lines().collect(Collectors.joining())).getAsJsonObject()
				
				retry = false;

			} catch (e : IOException) {
				LOGGER.error("Error: {}",e.message)
			}

		}

	return jsonObject

}


fun getReactions(tweetId : String, what : String) : List<Long>{

	LOGGER.debug("Processing {} {} {}",what, tweetId)
	
	val url = reactions_base_path.replace(_TWEET,tweetId).replace(_WHAT,what)
	
	LOGGER.debug("URL {}",url)
	
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
				LOGGER.error("ERROR: "+e.message);
				retry = false;
			}
		}
	
	LOGGER.debug("Gotten reactions {} {} {}",tweetId,what,reactions.size)

	return reactions
}

fun getSearchedTweet(text : String) : Long{
	
	val searchableText = "\"${text.trim().replace(" ","%20").replace("'","%27")}\"" //%22
	//.replace(' ', '%20').replace('#', '%23').replace(':', '%3A').replace('&', '%26')
	
	var cursor : String? = "0"
	val tweets = mutableMapOf<Long,String>()

	val urlText = search_path.replace(_TWEET,searchableText)
	
	while(cursor != null) {

		val url = urlText.replace(_POS,cursor)
	
		val jsonObject = getURLContent(url)!!;
				
		val html = jsonObject.get("items_html").getAsString()
		val doc = Jsoup.parse(html)!!

		tweets.putAll(parseSearch(doc))
				
		if(jsonObject.get("has_more_items").getAsBoolean())
			cursor = jsonObject.get("min_position").getAsString()
		else
			cursor = null
				
	}

	LOGGER.debug("Gotten search {} size: {}", text,tweets.size)
	
	//time to process the retrieved tweets!
	val closest = getClosestTweet(text,tweets)
	
	return closest	
}

private fun getClosestTweet(text : String, tweets : MutableMap<Long,String>) : Long {
	
	var closest = -1L
	var sim = Integer.MAX_VALUE
	var closest_text : String? = null
	
	tweets.forEach{
		val d = editDistance(text,it.value)
		println("$d ${it.value}")
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
	
	val urlPattern = "((https?|ftp|gopher|telnet|file|Unsure|http):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*)";
    val twiterUrlPattern = "([[\\S]*.]*(twitter.com)+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*)"
	
	doc.getAllElements().filter{it.className().startsWith("js-stream-item stream-item stream-item")}.forEach{
		
		val id = it.getElementsByAttribute("data-tweet-id").first().attr("data-tweet-id").toLong()
		
		val tweet = it.getElementsByClass("TweetTextSize  js-tweet-text tweet-text").first().text().toString().replace(Regex(urlPattern),"").replace(Regex(twiterUrlPattern),"")
		
		tweets.put(id,tweet)
		
	}
	
	return tweets
}

fun main(){
//	val screenname = "OfeFernandez_"
	val tweet_id = "1210306953936855043"
//	val list = getReplies(screenname,tweet_id)
	
	val list = getReactions(tweet_id,"retweeted")
	println(list);
	
	val text = "Ofelia Fernandéz presentó sus primeros proyectos como legisladora"
	print(getSearchedTweet(text))
}