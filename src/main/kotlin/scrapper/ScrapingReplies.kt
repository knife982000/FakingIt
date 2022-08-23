package scrapper

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

fun getPage(url : String, user_agent : String, bearer: String, guest_token : String, tweetId : String) : Triple<List<Long>,List<String>?,Long>{
	
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
	}
		
	if(content == null)
		return Triple<List<Long>,List<String>?,Long>(mutableListOf<Long>(),null,0)
	
	val objects = JsonParser().parse(content).getAsJsonObject()
	
//	println(objects)
//	println("-----------------------------")
	
	val tweets_json = objects.get("globalObjects").getAsJsonObject().get("tweets").getAsJsonObject()
		
	val replies = tweets_json.entrySet().map{it.key.toLong()}.toList()
	
	var cant_replies = 0L
	val searched = tweets_json.get(tweetId)
	if(searched != null)
		cant_replies = tweets_json.get(tweetId).getAsJsonObject().get("reply_count").getAsLong()
	
	if(replies.size <= 1)
		Triple<List<Long>,String?,Long>(mutableListOf<Long>(),null,cant_replies)
	
	val entries = objects.get("timeline").getAsJsonObject().get("instructions").getAsJsonArray()[0]
						 .getAsJsonObject()?.get("addEntries")?.getAsJsonObject()?.get("entries")?.getAsJsonArray()
	
//	val entries = objects.get("timeline").getAsJsonObject().get("instructions").getAsJsonArray().map{it}
//						 .mapNotNull{it.getAsJsonObject().get("addEntries")}
//						 .mapNotNull{it.getAsJsonObject().get("entries")}.mapNotNull{it.getAsJsonArray()}.flatMap{it}
//
	if (entries == null)
		return Triple<List<Long>,List<String>?,Long>(replies,null,cant_replies)
	
	val cursor = entries.mapNotNull{it.getAsJsonObject().get("content")}
		   .mapNotNull{it.getAsJsonObject().get("operation")}
		   .mapNotNull{it.getAsJsonObject().get("cursor")}
		   .mapNotNull{it.getAsJsonObject().get("value")}
		   .map{it.getAsString()}
		   .toList()//.getOrNull(0)
	
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
	
	
	return Triple<List<Long>,List<String>?,Long>(replies,cursor,cant_replies)
}

fun main(){
		
	val user_agent = "Mozilla/5.0 (X11; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/96.0"
	val tweetId = "111188989090909"
	
//	val query = "(@MunAdolfoAlsina)"
	
//	val url = "https://twitter.com/i/api/2/search/adaptive.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweet=true&tweet_search_mode=live&query_source=typed_query&pc=1&spelling_corrections=1&ext=mediaStats%2ChighlightedLabel&f=live&q=${URLEncoder.encode(query, "utf-8")}"
	val url = "https://twitter.com/i/api/2/timeline/conversation/$tweetId.json?include_reply_count=1&include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweet=true&referrer=tweet&count=20&include_ext_has_birdwatch_notes=false&ext=mediaStats%2ChighlightedLabel"
	
	val (bearer,guest_token) = getCredentials(user_agent)
	
	if(bearer == null || guest_token == null)
		return 
	
	val all_replies = mutableSetOf<Long>()
	var count = 20
//	var (replies,cursor,_) = getPage(url+"&cursor=DefaultBottomCursorValue&count=$count",user_agent,bearer,guest_token,tweetId)
	var (replies,cursor,_) = getPage(url,user_agent,bearer,guest_token,tweetId)
		
	all_replies.addAll(replies)
	
	var size = 0
		
	while(cursor != null){

		count += 20
		println(cursor)
		
//		var cc : String? = "DefaultBottomCursorValue"
//		if(size == all_replies.size){
			var cc = cursor.getOrNull(0)
//			if(cc == null || cc.startsWith("refresh"))
//				cc = cursor.getOrNull(1)
//			count = 20
//		}
		
		if(cc == null)
			break;
		
		size = all_replies.size
			
		println(cc)
		val triple = getPage(url+"&cursor=${URLEncoder.encode(cc, "utf-8")}",user_agent,bearer,guest_token,tweetId)
		cursor = triple.second
		all_replies.addAll(triple.first)
		
		println(all_replies.size)
		
	}

	all_replies.remove(tweetId.toLong())
	println(all_replies.size)
	println(all_replies)
	
//	var br = BufferedReader(InputStreamReader(FileInputStream("test_search.txt")))
//    val content = br.lines().collect(Collectors.joining())
//	
//	val objects = JsonParser().parse(content).getAsJsonObject()
//	
//	println(objects.keySet())
//	
//	val tweets_json = objects.get("globalObjects").getAsJsonObject().get("tweets").getAsJsonObject()
//	val replies = tweets_json.entrySet().map{it.key}.toList()
//
//	val entries = objects.get("timeline").getAsJsonObject().get("instructions").getAsJsonArray()[0].getAsJsonObject().
//		get("addEntries").getAsJsonObject().get("entries").getAsJsonArray()
//	
//
//		val cursor = entries.mapNotNull{it.getAsJsonObject().get("content")}
//		   .mapNotNull{it.getAsJsonObject().get("operation")}
//		   .mapNotNull{it.getAsJsonObject().get("cursor")}
//		   .mapNotNull{it.getAsJsonObject().get("value")}
//		   .map{it.getAsString()}
//		   .toList()
//	
//	println(cursor.getOrNull(1))
}