package utils;

import java.io.BufferedWriter;
import java.io.File;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.bson.types.ObjectId;

import edu.isistan.fakenews.storage.*;

public class RetrieveDataFromStorage {

	static MongoDBStorage storage;

	public static void 	traverseAndSaveTweets(String output_dir) throws IOException {

		int cant_tweets = (int) storage.tweets.estimatedDocumentCount();

		BufferedWriter out_tweets_created_place = Files.newBufferedWriter(Paths.get(output_dir+File.separator+"tweets_created_place.csv"));
		out_tweets_created_place.write("tweetId,createdAt,place");
		out_tweets_created_place.newLine();

		BufferedWriter out_tweets_media_url_contributors_mentions = Files.newBufferedWriter(Paths.get(output_dir+File.separator+"tweets_media_url_contributors_mentions.csv"));
		out_tweets_media_url_contributors_mentions.write("tweetId,media,url,contributors,mentions");
		out_tweets_media_url_contributors_mentions.newLine();

		BufferedWriter out_tweet_mentions = Files.newBufferedWriter(Paths.get(output_dir+File.separator+"tweets_mentions.csv"));
		out_tweet_mentions.write("tweetId,mentions");
		out_tweet_mentions.newLine();

		BufferedWriter out_tweet_url = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output_dir+File.separator+"tweets_urls.csv", false), "UTF8"));
		out_tweet_url.write("tweetId,url");
		out_tweet_url.newLine();

		BufferedWriter out_tweet_type = Files.newBufferedWriter(Paths.get(output_dir+File.separator+"tweets_type.csv"));
		out_tweet_type.write("tweetId,original,retweet,reply,quote");
		out_tweet_type.newLine();

		BufferedWriter out_tweet_type_ids = Files.newBufferedWriter(Paths.get(output_dir+File.separator+"tweets_type_ids.csv"));
		out_tweet_type_ids.write("tweetId,original,retweet,reply,quote");
		out_tweet_type_ids.newLine();

		BufferedWriter out_tweet_hashtags = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output_dir+File.separator+"tweets_hashtags.csv", false), "UTF8"));

		out_tweet_hashtags.write("tweetId,hashtags");
		out_tweet_hashtags.newLine();

		BufferedWriter out_tweet_user = Files.newBufferedWriter(Paths.get(output_dir+File.separator+"tweets_user.csv"));
		out_tweet_user.write("tweetId,userId");
		out_tweet_user.newLine();

		BufferedWriter out_tweet_reactions = Files.newBufferedWriter(Paths.get(output_dir+File.separator+"tweets_reactions.csv"));
		out_tweet_reactions.write("tweetId,favorites,retweets,replies");
		out_tweet_reactions.newLine();

		Date first = new Date(1577836800000l);
		Date last = new Date(1593561600000l);
		
		int i=0;
		Iterator<Tweet<ObjectId>> it = storage.tweets.find().iterator();
		while(it.hasNext()) {
			
			Tweet<ObjectId> tweet = it.next();

			if(i % 10_000 == 0)
				System.out.println("Processed "+i+" "+cant_tweets+" "+new Date());
			i++;
			
			//user -- filtering tweets with no user -- now, they are not included in any file!
			if(tweet.getUserId() < 0) {
//				System.out.println("Filtering tweet with no user: "+tweet.getTweetId());
				continue;
			}
		
			Date d = tweet.getCreated();
		
			if(d.before(first) || d.after(last)) { //1593561600000 July 1 2020 - 1577836800000 Jan 1 2020
//				System.out.println("Filtering "+d);
				continue;
			}
			
			long id = tweet.getTweetId();

			out_tweet_user.write(id+","+tweet.getUserId());
			out_tweet_user.newLine();
			
			out_tweets_created_place.write(id+","+tweet.getCreated().getTime()+","+((tweet.getPlace() != null) ? tweet.getPlace() : ""));
			out_tweets_created_place.newLine();

			boolean retweet = false;
			
//			//type
			if(tweet.getInReplyToStatusId() > 0) //it's a reply
				out_tweet_type.write(id+",0,0,1,"+((tweet.getQuotedStatusId() > 0) ? 1 : 0));
			else
				if(tweet.getRetweet()) //it's a retweet
					out_tweet_type.write(id+",0,1,0,0");
				else
					out_tweet_type.write(id+",1,0,0,"+((tweet.getQuotedStatusId() > 0) ? 1 : 0));
			out_tweet_type.newLine();
//
//			//type ids
			out_tweet_type_ids.write(id+",");
			if(tweet.getInReplyToStatusId() <= 0 && !tweet.getRetweet())
				out_tweet_type_ids.write("1,0,0,");
			else {
				if(tweet.getRetweet()) {
					retweet = true;
					out_tweet_type_ids.write("0,"+(tweet.getRetweetId() != null ? tweet.getRetweetId() : -1)+",0,");
				}
					
				else
					out_tweet_type_ids.write("0,0,"+tweet.getInReplyToStatusId()+",");
			}

			if(tweet.getQuotedStatusId() > 0)
				out_tweet_type_ids.write(Long.toString(tweet.getQuotedStatusId()));
			else
				out_tweet_type_ids.write("0");

			out_tweet_type_ids.newLine();

			if(!retweet) { //url, mentions and hashtags are already filtered
				List<UserEntity> mentions = tweet.getUserMentions();
				List<URLEntity> urls = tweet.getUrlEntities();
				
				//writing only if it contains at least one of the items
				if(mentions.size() > 0 || urls.size() > 0 || tweet.getMediaEntity().size() > 0 || tweet.getContributors().size() > 0) {
					out_tweets_media_url_contributors_mentions.write(id+","+tweet.getMediaEntity().size()+","
							+urls.size()+","
							+tweet.getContributors().size()+","
							+mentions.size());
					out_tweets_media_url_contributors_mentions.newLine();
				}
				
				if(mentions.size() > 0) {
					out_tweet_mentions.write(id+","+mentions.stream().map(UserEntity::getUserId).map(Object::toString).collect(Collectors.joining(" ")));
					out_tweet_mentions.newLine();				
				}

				if(urls.size() > 0) {
					out_tweet_url.write(id+","+urls.stream().map(URLEntity::getUrl).map(Object::toString).collect(Collectors.joining(" ")));
					out_tweet_url.newLine();
				}
				
				//hashtags
				List<BaseEntity> hashtags = tweet.getHastTags();
				StringBuilder sb = new StringBuilder();
				if(hashtags.size() > 0) {
					sb.setLength(0);
					Pattern pattern = Pattern.compile("#[A-Za-z0-9]+(?=[\\s|#|$])|#[A-Za-z0-9]+$");
					Matcher matcher = pattern.matcher(tweet.getText());
			        while (matcher.find()) {
			           sb.append(matcher.group());
			           sb.append(" ");
			        }
					
			        out_tweet_hashtags.write(id+","+sb.toString());
					
//					out_tweet_hashtags.write(id+","+hashtags.stream().map(h -> {
//						try {
//							return tweet.getText().substring(h.getStart(), h.getEnd()).toLowerCase();
//						}catch(StringIndexOutOfBoundsException e) {
//							return "";
//						}
//					}).collect(Collectors.joining(" ")));
					out_tweet_hashtags.newLine();	
				}
			}
	

			//reactions
			int favs = tweet.getFavoriteCount();
			int retweets = tweet.getRetweetCount();

			out_tweet_reactions.write(id+","+favs+","+retweets);
			out_tweet_reactions.newLine();

		}; //finished traversing tweets		

		out_tweets_created_place.close();
		out_tweets_media_url_contributors_mentions.close();
		out_tweet_mentions.close();
		out_tweet_url.close();
		out_tweet_type.close();
		out_tweet_hashtags.close();
		out_tweet_user.close();
		out_tweet_reactions.close();

		out_tweet_type_ids.close();

	}

	public static void traverseAndSavePlaces(String output_dir) throws IOException {
		System.out.println("Traversing places...");
		int cant = (int)storage.places.estimatedDocumentCount();
		//traverse places

		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output_dir+File.separator+"places.csv", false), "UTF8"));
		CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader("place_id","placeType","name","fullName","country","boundingBox","within"));

		int i=0;
		Iterator<Place<ObjectId>> it = storage.places.find().iterator();
		while(it.hasNext()){

			Place<ObjectId> place = it.next();

			if(i % 1000 == 0)
				System.out.println("Processed "+i+" "+cant+" "+new Date());
			i++;

			StringBuilder sb = new StringBuilder();
			BoundingBox bb = place.getBoundingBox();
			for(GeoLocation gl : bb.getLocation())
				sb.append("["+gl.getLatitude()+","+gl.getLongitude()+"],");
			if(sb.length() > 0)
				csvPrinter.printRecord(place.getPlaceId(),place.getPlaceType(),place.getName(),place.getFullName(),place.getCountry(),sb.substring(0, sb.length()-1),place.getWithin());
			else
			csvPrinter.printRecord(place.getPlaceId(),place.getPlaceType(),place.getName(),place.getFullName(),place.getCountry(),"",place.getWithin());
		}

		csvPrinter.flush();  
		csvPrinter.close();
		writer.close();

	}

	public static void traverseAndSaveUsers(String output_dir) throws IOException {
		System.out.println("Traversing users...");
		int cant = (int)storage.users.estimatedDocumentCount();
		int i = 0;
		
		BufferedWriter out_users = Files.newBufferedWriter(Paths.get(output_dir+File.separator+"users.csv"));
		CSVPrinter csvPrinter = new CSVPrinter(out_users, CSVFormat.DEFAULT.withHeader("userId","createdAt","location","hasProfileImage","followerCoun","friendCount","statusesCount","isVerified","favoritesCount","listedCount","geoEnabled"));

		Iterator<User<ObjectId>> it_user = storage.users.find().iterator();

		while(it_user.hasNext()) {
			User<ObjectId> user = it_user.next();
			if(i % 1000 == 0)
				System.out.println("Processed "+i+" "+cant+" "+new Date());
			i++;				

			List<Object> u = new ArrayList<>();
			
			u.add(user.getUserId());
			u.add(user.getCreated().getTime());
			if(user.getLocation() != null)
				u.add(user.getLocation().replaceAll("[\\r\\n]+", ""));
			else
				u.add("");
			u.add(!user.getDefaultProfileImage() ? 1 : 0);
			u.add(user.getFollowerCount());
			u.add(user.getFriendsCount());
			u.add(user.getStatusesCount());
			u.add(user.getVerified() ? 1 : 0);
			u.add(user.getFavoritesCount());
			u.add(user.getListedCount());
			u.add(user.getGeoEnabled() ? 1 : 0);

			csvPrinter.printRecord(u);

		}
		csvPrinter.close();
		out_users.close();
	}

	//Puede haber más de una fila para el mismo usuario
	public static void traverseAndSaveUserRelations(String output_dir) throws IOException{
		System.out.println("Traversing users relations...");
		int cant = (int)storage.users.estimatedDocumentCount();
		int i = 0;
		BufferedWriter out_user_relations = Files.newBufferedWriter(Paths.get(output_dir+File.separator+"users_followers.csv"));
		out_user_relations.write("userId,followers");
		out_user_relations.newLine();
		Iterator<UserRelations<ObjectId>> it_user = storage.userFollowers.find().iterator();
		while(it_user.hasNext()) {
			UserRelations<ObjectId> rels = it_user.next();
			if(i % 1000 == 0)
				System.out.println("Processed "+i+" "+cant+" "+new Date());
			i++;				

			out_user_relations.write(rels.getUserId()+","+rels.getRel().stream().map(Object::toString).collect(Collectors.joining(" ")));
			out_user_relations.newLine();

		}
		out_user_relations.close();

		out_user_relations = Files.newBufferedWriter(Paths.get(output_dir+File.separator+"users_followees.csv"));
		out_user_relations.write("userId,followees");
		out_user_relations.newLine();
		it_user = storage.userFollowees.find().iterator();
		while(it_user.hasNext()) {
			UserRelations<ObjectId> rels = it_user.next();
			if(i % 1000 == 0)
				System.out.println("Processed "+i+" "+cant+" "+new Date());
			i++;				

			out_user_relations.write(rels.getUserId()+","+rels.getRel().stream().map(Object::toString).collect(Collectors.joining(" ")));
			out_user_relations.newLine();

		}
		out_user_relations.close();

	}

	public static void traverseAndSaveFavoriters(String output_dir) throws IOException {
		System.out.println("Traversing favoriters...");
		BufferedWriter out_tweet_favoriters = Files.newBufferedWriter(Paths.get(output_dir+File.separator+"tweets_favoriters.csv"));
		out_tweet_favoriters.write("tweetId,favoriters");
		out_tweet_favoriters.newLine();

		Iterator<TweetReactions<ObjectId>> it = storage.tweetFavorites.find().iterator();
		while(it.hasNext()) {
			TweetReactions<ObjectId> r = it.next();
			List<Long> users = r.getUsers();
			if(users.size() > 0) {
				out_tweet_favoriters.write(r.getTweetId()+","+users.stream().map(Object::toString).collect(Collectors.joining(" ")));
				out_tweet_favoriters.newLine();
			}	
		}

		out_tweet_favoriters.close();
	}

	public static void traverseAndSaveRetweeters(String output_dir) throws IOException {
		System.out.println("Traversing retweeters...");
		BufferedWriter out_tweet_retweeters = Files.newBufferedWriter(Paths.get(output_dir+File.separator+"tweets_retweeters.csv"));
		out_tweet_retweeters.write("tweetId,retweeters");
		out_tweet_retweeters.newLine();

		Iterator<TweetReactions<ObjectId>> it = storage.tweetRetweeters.find().iterator();
		while(it.hasNext()) {
			TweetReactions<ObjectId> r = it.next();
			List<Long> users = r.getUsers();
			if(users.size() > 0) {
				out_tweet_retweeters.write(r.getTweetId()+","+users.stream().map(Object::toString).collect(Collectors.joining(" ")));
				out_tweet_retweeters.newLine();
			}	
		}

		out_tweet_retweeters.close();

	}

	public static void traverseAndSaveReplies(String output_dir) throws IOException {		
		System.out.println("Traversing replies...");
		BufferedWriter out_tweet_replies = Files.newBufferedWriter(Paths.get(output_dir+File.separator+"tweets_replies.csv"));
		out_tweet_replies.write("tweetId,replies");
		out_tweet_replies.newLine();

		Iterator<TweetReplies<ObjectId>> it = storage.tweetReplies.find().iterator();
		while(it.hasNext()) {
			TweetReplies<ObjectId> re = it.next();
			List<Long> reps = re.getReplies();
			if(reps.size() > 0) {
				out_tweet_replies.write(re.getTweetId()+","+re.getReplies().stream().map(Object::toString).collect(Collectors.joining(" ")));
				out_tweet_replies.newLine();
			}	
		}

		out_tweet_replies.close();
	}

	public static void traverseAndSaveQueryTweets(String output_dir) throws IOException {
		System.out.println("Traversing queries...");
		BufferedWriter out_tweet_queries = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output_dir+File.separator+"tweets_queries.txt", false), "UTF8"));

		int cant = (int)storage.queries.estimatedDocumentCount();
		int i = 0;
		Iterator<Query<ObjectId>> it = storage.queries.find().iterator();
			
		while(it.hasNext()) {

			if(i % 100 == 0)
				System.out.println("Processed "+i+" "+cant+" "+new Date());
			i++;

			Query<ObjectId> query = it.next();
			List<Long> ids = query.getTweetIds();
			
			out_tweet_queries.write(ids.stream().map(l -> l+"").collect(Collectors.joining(System.lineSeparator())));
			out_tweet_queries.newLine();
		}

		out_tweet_queries.close();
	}

	public static void main(String[] args) throws IOException {
		String output_dir = args[0];
	
		System.setProperty("ddb.name", args[1]);
		
		storage = new MongoDBStorage();
		traverseAndSaveTweets(output_dir);
		traverseAndSavePlaces(output_dir);
		traverseAndSaveUsers(output_dir);
		traverseAndSaveUserRelations(output_dir);
		traverseAndSaveFavoriters(output_dir);
		traverseAndSaveRetweeters(output_dir);
		traverseAndSaveReplies(output_dir);
		traverseAndSaveQueryTweets(output_dir);
		storage.close();

	}

}
