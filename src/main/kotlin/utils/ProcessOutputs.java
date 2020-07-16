package utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

public class ProcessOutputs {

	//	static Map<Long,Integer> tweets_types = null;
	static Long2IntMap tweets_types;
	static LongSet verified_users;

	//Filters all RT from the analysis!
	public static void transformTweetType(String output_dir) throws IOException {
		Reader reader = Files.newBufferedReader(Paths.get(output_dir+File.separator+"tweets_type.csv"));
		CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader()); 

		BufferedWriter writer = Files.newBufferedWriter(Paths.get(output_dir+File.separator+"_tweet_type2.csv"));
		CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader("tweetId","original","retweet","reply","quote"));

		for (CSVRecord csvRecord : csvParser){
			long l = Long.parseLong(csvRecord.get("tweetId"));
			List<Object> values = new ArrayList<>();
			//			 values.add(Long.toUnsignedString(l, 32));
			values.add(l);
			if(csvRecord.get("type").equals("original")) {
				values.add(1);
				values.add(0);
				values.add(0);
			}else
				if(csvRecord.get("type").equals("retweet")) {
					values.add(0);
					values.add(1);
					values.add(0);
				}
				else
					if(csvRecord.get("type").equals("reply")) {
						values.add(0);
						values.add(0);
						values.add(1);
					}
			if(csvRecord.get("isQuote").equals("TRUE"))
				values.add(1);
			else
				values.add(0);


			csvPrinter.printRecord(values); 
		}

		csvPrinter.flush();
		csvPrinter.close();
		writer.close();

		csvParser.close();
		reader.close();
	}

	public static Map<Long,Integer> filterRT(String output_dir) throws IOException {

		System.out.println("--------------------- filterRT");

		Reader reader = Files.newBufferedReader(Paths.get(output_dir+File.separator+"tweets_type.csv"));
		CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader()); 

		int original = 0;
		int retweets = 0;
		int replies = 0;
		int quotes = 0;
		int total = 0;

		int original_quotes = 0;
		int replies_quotes = 0;

		LongSet toKeep = new LongOpenHashSet(80_000_000);  
		tweets_types = new Long2IntOpenHashMap(110_000_000,0.90f);

		for (CSVRecord csvRecord : csvParser){

			if(total % 100_000 == 0)
				System.out.println(total+" "+new Date());

			long tid = Long.parseLong(csvRecord.get("tweetId"));

			if(Integer.parseInt(csvRecord.get("original")) != 0) {
				original++;
				tweets_types.put(tid, 1);
				toKeep.add(tid);
			}

			else
				if(!csvRecord.get("retweet").equals("null") && Integer.parseInt(csvRecord.get("retweet")) != 0) {
					tweets_types.put(tid, 2);
					retweets++;
				}
				else {
					toKeep.add(tid);
					tweets_types.put(tid, 3);
					replies++;
				}

			if(Integer.parseInt(csvRecord.get("quote")) != 0) {
				quotes++;
				if(Integer.parseInt(csvRecord.get("original")) != 0)
					original_quotes++;
				else
					replies_quotes++;
			}
			total++;
		}

		BufferedWriter out = Files.newBufferedWriter(Paths.get(output_dir+File.separator+"_filtered_tweets.csv"));
		out.write(toKeep.stream().map(l -> Long.toString(l)).collect(Collectors.joining(System.lineSeparator())));
		out.close();

		System.out.println("total,original,retweets,replies,quotes,original-quotes,replies-quotes");
		System.out.println(total+","+original+","+retweets+","+replies+","+quotes+","+original_quotes+","+replies_quotes);
		System.out.println(","+original/(float)total+","+retweets/(float)total+","+replies/(float)total+","+quotes/(float)total);

		csvParser.close();
		reader.close();

		return tweets_types;
	}

	public static LongSet tweetsInQuery(String output_dir) throws IOException{
		LongSet ids = new LongOpenHashSet(80_000_000);

		if(new File(output_dir+File.separator+"_tweets_queries_filtered.txt").exists()) {
			BufferedReader reader = Files.newBufferedReader(Paths.get(output_dir+File.separator+"_tweets_queries_filtered.txt"));
			String l = reader.readLine();
			while(l != null) {
				ids.add(Long.parseLong(l));
				l = reader.readLine();
			}
			reader.close();
			return ids;
		}

		int i=0;
		BufferedReader reader = Files.newBufferedReader(Paths.get(output_dir+File.separator+"tweets_queries.txt"));
		String l = reader.readLine();
		while(l != null) {
			if(i % 1_000_000 == 0)
				System.out.println(i+" "+new Date());
			i++;
			ids.add(Long.parseLong(l));
			l = reader.readLine();
		}
		reader.close();

		BufferedWriter writer = Files.newBufferedWriter(Paths.get(output_dir+File.separator+"_tweets_queries_filtered.txt"));
		writer.write(ids.stream().map(t -> Long.toString(t)).collect(Collectors.joining(System.lineSeparator())));
		writer.close();
		return ids;
	}

	//get tweets por día	
	public static void processDates(String output_dir) throws IOException {

		System.out.println("--------------------- processDates");

		if(tweets_types == null)
			filterRT(output_dir);

		Reader reader = Files.newBufferedReader(Paths.get(output_dir+File.separator+"tweets_created_place.csv"));
		CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader()); 

		//		LongSet tweets_query = tweetsInQuery(output_dir);
		DateTimeFormatter formatter = DateTimeFormatter.ofLocalizedDate(FormatStyle.SHORT);

		Map<String,LongSet> dates = new HashMap<>();
		//		Map<String,LongSet> dates_query = new HashMap<>();

		@SuppressWarnings("unchecked")
		Map<String,LongSet> [] dates_type = new HashMap[3];
		dates_type[0] = new HashMap<String,LongSet>();
		dates_type[1] = new HashMap<String,LongSet>();
		dates_type[2] = new HashMap<String,LongSet>();

		int i = 0;
		for(CSVRecord record : csvParser) {

			if(i % 1_000_000 == 0)
				System.out.println(i+" "+new Date());
			i++;

			long tid = Long.parseLong(record.get("tweetId"));
			int type = tweets_types.get(tid)-1;

			Instant instant = Instant.ofEpochMilli(Long.parseLong(record.get("createdAt")));
			ZonedDateTime zdt = ZonedDateTime.ofInstant(instant,ZoneOffset.UTC);
			String dateFormatted = formatter.format (zdt);

			LongSet ids = dates.get(dateFormatted);
			//			LongSet ids_query = dates_query.get(dateFormatted);

			LongSet ids_type = dates_type[type].get(dateFormatted);

			if(ids == null) {
				ids = new LongOpenHashSet();
				dates.put(dateFormatted, ids);

				//				ids_query = new LongOpenHashSet();
				//				dates_query.put(dateFormatted, ids_query);
			}

			ids.add(tid);
			//			if(tweets_query.contains(tid))
			//				ids_query.add(tid);

			if(ids_type == null) {
				ids_type = new LongOpenHashSet();
				dates_type[type].put(dateFormatted, ids_type);
			}
			ids_type.add(tid);

		}
		csvParser.close();
		reader.close();

		BufferedWriter out = Files.newBufferedWriter(Paths.get(output_dir+File.separator+"_created_at.csv"));
		//		out.write("date,total_tweets,query_tweets,total_original,total_retweet,total_reply");
		out.write("date,total_tweets,total_original,total_retweet,total_reply");
		out.newLine();
		out.write(dates.entrySet().stream().sorted(Map.Entry.comparingByKey())
				.map(k -> k.getKey()+","+k.getValue().size()
						//						+","+dates_query.getOrDefault(k.getKey(), new LongOpenHashSet()).size()
						+","+dates_type[0].getOrDefault(k.getKey(), new LongOpenHashSet()).size()
						+","+dates_type[1].getOrDefault(k.getKey(), new LongOpenHashSet()).size()
						+","+dates_type[2].getOrDefault(k.getKey(), new LongOpenHashSet()).size()
						)
				.collect(Collectors.joining(System.lineSeparator())));
		out.close();

	}

	//Create Table for sharing... instead of the place id, the actual place adds to new columns with the full name and the country
	public static void createTableTweetCreatedAtPlace(String output_dir) throws IOException  {

		System.out.println("--------------------- createTableTweetCreatedAtPlace");

		Map<String,String[]> places = loadPlaces(output_dir+File.separator+"places.csv");

		Reader reader = new BufferedReader(new InputStreamReader(new FileInputStream(output_dir+File.separator+"tweets_created_place.csv"), "UTF8"));
		CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader()); 

		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output_dir+File.separator+"_tweets_created_place.csv", false), "UTF8"));
		CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader("tweetId","createdAt","place_fullName","place_country"));

		for(CSVRecord record : csvParser) {
			if(record.get("place") == null || record.get("place").equals("null") || record.get("place").isEmpty()) {
				csvPrinter.printRecord(record.get("tweetId"),record.get("createdAt"));
				continue;
			}

			String [] desc_place = places.get(record.get("place"));
			if(desc_place == null) {
				csvPrinter.printRecord(record.get("tweetId"),record.get("createdAt"));
				continue;
			}
			csvPrinter.printRecord(record.get("tweetId"),record.get("createdAt"),desc_place[0],desc_place[1]);
		}

		csvPrinter.close();
		writer.close();

		csvParser.close();
		reader.close();
	}

	private static Map<String, String[]> loadPlaces(String path) throws IOException {

		System.out.println("--------------------- loadPlaces");

		Reader reader = Files.newBufferedReader(Paths.get(path));
		CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader()); 

		Map<String,String[]> places = new HashMap<>();

		for(CSVRecord record : csvParser) {
			String [] vals = new String[2];
			vals[0] = record.get("fullName");
			vals[1] = record.get("country");
			places.put(record.get("place_id"), vals);
		}

		//		System.out.println(places.toString());

		csvParser.close();
		reader.close();

		return places;

	}

	//solo nos importan los ids de los tweets
	public static void getTweetsVerifiedUsers(String output_dir, int cant) throws IOException {

		System.out.println("--------------------- getTweetsVerifiedUsers "+cant);

		if(verified_users == null)
			verified_users = getVerifiedUsers(output_dir+File.separator+"users.csv");

		if(cant > 0) { //need to filter the verified_users to only get the N higher
			Reader reader = Files.newBufferedReader(Paths.get(output_dir+File.separator+"_users_tweets_type.csv"));
			CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader()); 

			Long2IntMap aux = new Long2IntOpenHashMap();
			for(CSVRecord record : csvParser) 
				aux.put(Long.parseLong(record.get("userId")),Integer.parseInt(record.get("original")));

			csvParser.close();
			reader.close();

			List<Long> filtered = aux.long2IntEntrySet().stream()
					.filter(it -> verified_users.contains(it.getLongKey()))
					.sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
					.map(it -> it.getLongKey()).collect(Collectors.toList());		// verified users sorted by posted tweets	
			
			if(filtered.size() <= cant)
				cant = filtered.size();
			
			int cants = aux.get(filtered.get(cant-1).longValue());
			
			verified_users.clear();
			verified_users.addAll(filtered.stream().filter(it -> aux.get(it.longValue()) > cants).collect(Collectors.toSet()));
					
			System.out.println(cants+" "+verified_users.size());

		}

		Set<Long> filtered_tweets = null;
		if(tweets_types != null)
			filtered_tweets = tweets_types.long2IntEntrySet().stream().filter(it -> it.getIntValue() != 2).map(it -> it.getLongKey()).collect(Collectors.toSet());
		else
			filtered_tweets = loadFilteredTweets(output_dir);

		Reader reader = Files.newBufferedReader(Paths.get(output_dir+File.separator+"tweets_user.csv"));
		CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader()); 

		Set<Long> toKeep = new HashSet<>();

		for(CSVRecord record : csvParser) {
			long id = Long.parseLong(record.get("tweetId"));
			if(verified_users.contains(Long.parseLong(record.get("userId"))) && filtered_tweets.contains(id))
				toKeep.add(id);
		}

		csvParser.close();
		reader.close();

		String name = null;
		if(cant < 0)
			name = output_dir+File.separator+"_tweets_verified_users.csv";
		else
			name = output_dir+File.separator+"_tweets_verified_users_"+cant+".csv";

		BufferedWriter out = Files.newBufferedWriter(Paths.get(name));
		out.write(toKeep.stream().map(l -> Long.toString(l)).collect(Collectors.joining(System.lineSeparator())));
		out.close();
	}

	private static Set<Long> loadFilteredTweets(String output_dir) throws IOException {
		Set<Long> tweets = new HashSet<>();
		BufferedReader reader = Files.newBufferedReader(Paths.get(output_dir+File.separator+"_filtered_tweets.csv"));
		String l = reader.readLine();
		while(l != null) {
			tweets.add(Long.parseLong(l));
			l = reader.readLine();
		}
		reader.close();
		return tweets;
	}

	private static LongSet getVerifiedUsers(String path) throws IOException {

		System.out.println("--------------------- getVerifiedUsers");

		Reader reader = Files.newBufferedReader(Paths.get(path));
		CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader()); 
		LongSet users = new LongOpenHashSet();

		for(CSVRecord record : csvParser) {
			if(record.get("isVerified").equals("1") || record.get("isVerified").equals("true") )
				users.add(Long.parseLong(record.get("userId")));
		}

		csvParser.close();
		reader.close();
		return users;
	}

	//output table + stats for figure (?)
	public static void createTableTweetMediaURL(String output_dir) throws IOException  {

		System.out.println("--------------------- createTableTweetMediaURL");

		if(tweets_types == null)
			filterRT(output_dir);

		Reader reader = Files.newBufferedReader(Paths.get(output_dir+File.separator+"tweets_media_url_contributors_mentions.csv"));
		CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader()); 

		int [] original = new int[7]; //total,media,url,media-url,contributors,mentions,contributors-mentions
		int [] replies = new int[7]; //total,media,url,media-urlcontributors,mentions,contributors-mentions

		for(CSVRecord record : csvParser) {
			long id = Long.parseLong(record.get("tweetId"));
			int type = tweets_types.get(id);
			if(type != 2) { //si no es retweet

				int [] fill = (type == 1) ? original : replies;
				fill[0]++;

				int min = (type == 1) ? 0 : 1;

				int med = Integer.parseInt(record.get("media"));
				int url = Integer.parseInt(record.get("url"));
				int cont = Integer.parseInt(record.get("contributors"));
				int ment = Integer.parseInt(record.get("mentions"));

				if(med > 0 && url > 0)
					fill[3]++;
				else
					if(med > 0)
						fill[1]++;
					else
						if(url > 0)
							fill[2]++;

				if(cont > 0 && ment > min)
					fill[6]++;
				else
					if(cont > 0)
						fill[4]++;
					else
						if(ment > min)
							fill[5]++;
			}
		}

		csvParser.close();
		reader.close();

		System.out.println(",total,media,url,contributors,media-url,mentions,contributors-mentions");
		System.out.println("original,"+Arrays.toString(original).replace("[", "").replace("]", ""));
		System.out.println("replies,"+Arrays.toString(replies).replace("[", "").replace("]", ""));

	}

	//output table + stats for figure (?) user - cantidad de tweets por usuario total,original,retweet,reply
	//separado en deciles ? para ver la distribución ?
	public static void createTableUserTweets(String output_dir) throws IOException  {

		System.out.println("--------------------- createTableUserTweets");

		if(tweets_types == null)
			filterRT(output_dir);

		Map<Long,int[]> users_tweets = new HashMap<>(1_000_000,0.8f);

		Reader reader = Files.newBufferedReader(Paths.get(output_dir+File.separator+"tweets_user.csv"));
		CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader()); 

		for(CSVRecord record : csvParser) {
			long tid = Long.parseLong(record.get("tweetId"));

			int type = tweets_types.get(tid);
			if(type == 0)
				continue;

			long uid = Long.parseLong(record.get("userId"));

			int [] tt = users_tweets.get(uid);
			if(tt == null) {
				tt = new int[4];
				users_tweets.put(uid, tt);
			}
			tt[0]++;
			tt[type]++;			
		}
		csvParser.close();
		reader.close();

		//saving users and quantities...
		BufferedWriter writer = Files.newBufferedWriter(Paths.get(output_dir+File.separator+"_users_tweets_type.csv"));
		CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader("userId","total","original","retweet","reply"));

		double [] all_values = new double[users_tweets.size()];
		int i=0;

		for(Entry<Long,int[]> u : users_tweets.entrySet()) {

			int [] a = u.getValue();
			csvPrinter.printRecord(new Object[] {u.getKey(),a[0],a[1],a[2],a[3]});
			all_values[i] = a[0];
			i++;
		}
		csvPrinter.close();
		writer.close();

		//deciles y quartiles para la distribución...
		DescriptiveStatistics stats = new DescriptiveStatistics(all_values);
		stats.setPercentileImpl(new Percentile().withEstimationType(Percentile.EstimationType.R_2 ));

		double [] quartiles = new double[4];
		quartiles[0] = stats.getPercentile(25);
		quartiles[1] = stats.getPercentile(50);
		quartiles[2] = stats.getPercentile(75);
		quartiles[3] = stats.getPercentile(100);

		double [] deciles = new double[10];
		for(int j=1;j<=10;j++)
			deciles[j-1] = stats.getPercentile(j*10);

		System.out.println(Arrays.toString(quartiles));
		System.out.println(Arrays.toString(deciles));

		int [][] quartiles_cants = new int[4][4];
		int [][] deciles_cants = new int[10][4];

		IntList all = new IntArrayList();


		for(Entry<Long,int[]> u : users_tweets.entrySet()) {
			int [] a = u.getValue();
			all.add(a[0]);
			int j = 0;
			while(j < quartiles.length && quartiles[j] < a[0])
				j++;
			if(j == quartiles.length)
				j--;
			quartiles_cants[j][0] += a[0];
			quartiles_cants[j][1] += a[1];
			quartiles_cants[j][2] += a[2];
			quartiles_cants[j][3] += a[3];

			j = 0;
			while(j < deciles.length && deciles[j] < a[0])
				j++;
			if(j == deciles.length)
				j--;
			deciles_cants[j][0] += a[0];
			deciles_cants[j][1] += a[1];
			deciles_cants[j][2] += a[2];
			deciles_cants[j][3] += a[3];
		}

		System.out.println("quartile,total,original,retweet,reply");
		for(int j=0;j<quartiles_cants.length;j++)
			System.out.println(quartiles[j]+","+Arrays.toString(quartiles_cants[j]).replace("[", "").replace("]",""));

		System.out.println("decile,total,original,retweet,reply");
		for(int j=0;j<deciles_cants.length;j++)
			System.out.println(deciles[j]+","+Arrays.toString(deciles_cants[j]).replace("[", "").replace("]",""));

		System.out.println(all.stream().collect(Collectors.summarizingInt(Integer::intValue)));

	}

	//filter users without tweets from the users.csv file --> it might happen as we got followees/followers for some users
	public static void filterUsersWithTweets(String output_dir) throws IOException {

		System.out.println("--------------------- filterUsersWithTweets");

		BufferedReader reader2 = Files.newBufferedReader(Paths.get(output_dir+File.separator+"_users_tweets_type.csv")); //everyuser here must remain in the other file
		LongSet users = new LongOpenHashSet();
		reader2.readLine();
		String l = reader2.readLine();
		while(l != null) {
			users.add(Long.parseLong(l.substring(0,l.indexOf(","))));
			l = reader2.readLine();
		}
		reader2.close();

		BufferedReader reader = Files.newBufferedReader(Paths.get(output_dir+File.separator+"users.csv"));
		BufferedWriter writer = Files.newBufferedWriter(Paths.get(output_dir+File.separator+"_users_filtered.csv"));

		l = reader.readLine();
		writer.write(l);
		writer.newLine();

		l = reader.readLine();
		while(l != null) {
			try {
				if(users.contains(Long.parseLong(l.substring(0,l.indexOf(","))))) {
					writer.write(l);
					writer.newLine();
				}	
			}catch(Exception e) {
				System.out.println(l);
			}

			l = reader.readLine();
		}

		writer.close();
		reader.close();

	}

	//usuario,type,users --> en función de las menciones,replies,retweets... 3 archivos. Retweets y replies de la tabla de tweets - Menciones de la de menciones - Replies de la tabla de replies (just in case)
	public static void createTableUserRelations(String output_dir) throws IOException  {

		System.out.println("--------------------- createTableUserRelations");

		Long2LongMap tweet_user = new Long2LongOpenHashMap(108_000_000);
		Reader reader = Files.newBufferedReader(Paths.get(output_dir+File.separator+"tweets_user.csv"));
		CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader()); 
		int k = 0;
		for(CSVRecord record : csvParser) {
			if(k % 100_000 == 0)
				System.out.println(k+" "+new Date());
			k++;
			tweet_user.put(Long.parseLong(record.get("tweetId")), Long.parseLong(record.get("userId")));
		}

		csvParser.close();
		reader.close();

		BufferedWriter writer = Files.newBufferedWriter(Paths.get(output_dir+File.separator+"_users_graph.csv"));
		CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader("userId","relation","ids"));

		//este es directo
		System.out.println("----------- tweets_mentions");
		reader = Files.newBufferedReader(Paths.get(output_dir+File.separator+"tweets_mentions.csv")); //de acá saco tweet id de las relaciones
		csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader()); 
		for(CSVRecord record : csvParser) {
			long uid = tweet_user.get(Long.parseLong(record.get("tweetId")));
			if(uid == 0)
				continue;

			csvPrinter.printRecord(new Object[] {uid,"4",record.get("mentions")});

		}
		csvPrinter.flush();
		csvParser.close();
		reader.close();

		StringBuilder sb = new StringBuilder();
		System.out.println("----------- tweet_replies");
		reader = Files.newBufferedReader(Paths.get(output_dir+File.separator+"tweets_replies.csv")); //de acá saco tweet id de las relaciones
		csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader()); 
		for(CSVRecord record : csvParser) {
			long uid = tweet_user.get(Long.parseLong(record.get("tweetId")));
			if(uid == 0)
				continue;

			sb.setLength(0);

			String [] sp = record.get("replies").split(" ");
			for(String s : sp) {
				long ud = tweet_user.get(Long.parseLong(s));
				if(ud != 0) {
					sb.append(ud);
					sb.append(" ");
				}
				csvPrinter.printRecord(new Object[] {uid,"2",sb.toString()});	
			}	
		}
		csvPrinter.flush();
		csvParser.close();
		reader.close();

		//		Map<Long,LongSet[]> user_relations = new HashMap<>(11_000_000,0.9f); //< user, [retweet,reply,quoted,mentioned] >  --> reply >> quoted

		System.out.println("----------- tweets_type_ids");
		reader = Files.newBufferedReader(Paths.get(output_dir+File.separator+"tweets_type_ids.csv")); //de acá saco tweet id de las relaciones
		csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader()); 
		k = 0;
		for(CSVRecord record : csvParser) {

			if(k % 100_000 == 0)
				System.out.println(k+" "+new Date());
			k++;

			LongSet [] ids = null;
			long tid = Long.parseLong(record.get("tweetId"));

			long ret = Long.parseLong(record.get("retweet"));
			long rer = Long.parseLong(record.get("reply"));	
			long quot = Long.parseLong(record.get("quote"));
			if(ret > 0 || rer > 0 || quot > 0) {

				long retu = tweet_user.get(ret);
				long reru = tweet_user.get(rer);
				long quotu = tweet_user.get(quot);

				//				if(retu > 0 || reru > 0 || quotu > 0) {
				//					ids = user_relations.get(tweet_user.get(tid));
				//					if(ids == null) {
				//						ids = new LongOpenHashSet[4];
				//						ids[0] = new LongOpenHashSet(1_000);
				//						ids[1] = new LongOpenHashSet(1_000);
				//						ids[2] = new LongOpenHashSet(1_000);
				//						ids[3] = new LongOpenHashSet(1_000);	
				//						user_relations.put(tweet_user.remove(tid), ids);
				//					}
				//					
				//					if(retu > 0) ids[0].add(retu);
				//					if(reru > 0) ids[1].add(reru);
				//					if(quotu > 0) ids[2].add(quotu);
				//				}

				if(retu > 0)
					csvPrinter.printRecord(new Object[] {tid,"1",retu});

				if(reru > 0)
					csvPrinter.printRecord(new Object[] {tid,"2",reru});

				if(quotu > 0)
					csvPrinter.printRecord(new Object[] {tid,"3",quotu});

			}

		}
		csvParser.close();
		reader.close();

		//save the relations
		System.out.println("----------- _users_graph");

		//		for(Entry<Long,LongSet []> e : user_relations.entrySet()) {
		//			Set<Long> [] ids = e.getValue();
		//			for(int i=0;i<ids.length;i++) {
		//				if(ids[i].size() == 0)
		//					continue;
		//				
		//				csvPrinter.printRecord(new Object[] {e.getKey(),
		////													(i == 0) ? "retweet" : (i == 1) ? "reply" : (i == 2) ? "quoted" : "mentioned",
		//													(i == 0) ? "1" : (i == 1) ? "2" : (i == 2) ? "3" : "4",
		//													ids[i].stream().map(it -> Long.toString(it)).collect(Collectors.joining(" "))
		//													});
		//			}
		//		}
		csvPrinter.close();
		writer.close();

		//		int [] users = new int[4]; // [retweet,reply,quoted,mentioned]
		//		int [] totals = new int[4];
		//		for(Entry<Long,LongSet[]> user : user_relations.entrySet()) {
		//			LongSet [] rels = user.getValue();
		//			for(int i=0;i<rels.length;i++)
		//				if(rels[i].size() > 0) {
		//					users[i]++;
		//					totals[i] += rels[i].size(); 
		//				}
		//		}
		//		System.out.println(",retweet,reply,quoted,mentioned");
		//		System.out.println("coverage_users,"+Arrays.toString(users).replace("[", "").replace("]",""));
		//		System.out.println("totals,"+Arrays.toString(totals).replace("[", "").replace("]",""));
	}

	public static void filterHashtags(String output_dir) throws IOException {

		System.out.println("--------------------- filterHashtags");

		Set<Long> filtered_tweets = null;
		if(tweets_types != null)
			filtered_tweets = tweets_types.long2IntEntrySet().stream().filter(it -> it.getIntValue() != 2).map(it -> it.getLongKey()).collect(Collectors.toSet());
		else
			filtered_tweets = loadFilteredTweets(output_dir);

		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output_dir+File.separator+"tweets_hashtags_filtered.csv", false), "UTF8"));
		CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader("tweetId","hashtags"));

		Reader reader = new BufferedReader(new InputStreamReader(new FileInputStream(output_dir+File.separator+"tweets_hashtags.csv"), "UTF8"));
		CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader()); 

		for(CSVRecord record : csvParser) {

			try {
				if(!filtered_tweets.contains(Long.parseLong(record.get("tweetId"))))
					continue;
			}catch(NumberFormatException e) {
				continue;
			}

			csvPrinter.printRecord(new Object[]{record.get("tweetId"),record.get("hashtags")});
		}

		csvParser.close();
		reader.close();

		csvPrinter.close();
		writer.close();
	}


	//output table + stats for figure (?)
	public static void createTableTweetHashtags(String output_dir) throws IOException  {

		System.out.println("--------------------- createTableTweetHashtags");

		Map<String,Integer> hashtags = new HashMap<>(); // hashtag usage <hashtag,freq>
		Map<Integer,Integer> freq = new HashMap<>(); // <cant hashtags per tweet, freq>

		// hashtags is already filtered
		Reader reader = new BufferedReader(new InputStreamReader(new FileInputStream(output_dir+File.separator+"tweets_hashtags.csv"), "UTF8"));
		CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader()); 

		for(CSVRecord record : csvParser) {

			if(record.size() > 1) {
				String [] hhs = record.get("hashtags").split(" ");

				freq.merge(hhs.length, 1, (prev,one) -> prev + one);

				for(String h : hhs) 
					hashtags.merge(h, 1, (prev,one) -> prev + one);
			}


		}

		csvParser.close();
		reader.close();

		//sorting...
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output_dir+File.separator+"_hashtag_freq.csv", false), "UTF8"));
		writer.write(hashtags.entrySet().stream().sorted(Collections.reverseOrder(Map.Entry.comparingByValue())).map(it -> it.getKey()+","+it.getValue()).collect(Collectors.joining(System.lineSeparator())));
		writer.close();

		writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output_dir+File.separator+"_hashtag_tweets.csv", false), "UTF8"));
		writer.write(freq.entrySet().stream().sorted(Map.Entry.comparingByKey()).map(it -> it.getKey()+","+it.getValue()).collect(Collectors.joining(System.lineSeparator())));
		writer.close();

	}

	public static void fileLongConversion(String output_dir) throws IOException {

		//simple files with one or two columns
		convertSimpleFiles(output_dir,"_tweets_created_place.csv","comp_tweets_created_place.csv");
		convertSimpleFiles(output_dir,"_users_filtered.csv","comp_users_filtered.csv");
		convertSimpleFiles(output_dir,"tweets_user.csv","comp_tweets_user.csv");
		convertSimpleFiles(output_dir,"tweets_hashtags.csv","comp_tweets_hashtags.csv");
		convertSimpleFiles(output_dir,"tweets_type_ids.csv","comp_tweets_type_ids.csv");
		convertSimpleFiles(output_dir,"tweets_media_url_contributors_mentions.csv","comp_tweets_media_url_contributors_mentions.csv");
		convertSimpleFiles(output_dir,"_tweetd_user_createdAt.csv","comp_tweetd_user_createdAt.csv");
		convertSimpleFiles(output_dir,"_users_graph_merged.csv","comp_users_graph_merged.csv");

		//files with lists
		convertsMultiFiles(output_dir,"tweets_mentions.csv","comp_tweets_mentions.csv");
		convertsMultiFiles(output_dir,"tweets_replies.csv","comp_tweets_replies.csv");


	}

	private static void convertsMultiFiles(String output_dir, String original_name, String dest_name)
			throws UnsupportedEncodingException, FileNotFoundException, IOException {

		System.out.println(" ---------- convertMultiFiles "+original_name);

		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(output_dir+File.separator+original_name), "UTF8"));

		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output_dir+File.separator+dest_name, false), "UTF8"));
		writer.write(reader.readLine());
		writer.newLine();

		int i=0;

		String l = reader.readLine();
		while(l != null) {

			if(i % 10_000 == 0)
				System.out.println(i+" "+new Date());
			i++;

			String [] sp = l.split(",");
			StringBuilder sb = new StringBuilder();
			sb.append(Long.toString(Long.parseLong(sp[0]),Character.MAX_RADIX));
			sb.append(",");

			for(int j=1;j<sp.length-1;j++)
				sb.append(sp[j]+",");

			for(String c : sp[sp.length-1].split(" ")) {
				sb.append(Long.toString(Long.parseLong(c),Character.MAX_RADIX));
				sb.append(" ");
			}
			writer.write(sb.toString());
			writer.newLine();

			l = reader.readLine();
		}

		writer.close();
		reader.close();
	}

	private static void convertSimpleFiles(String output_dir, String original_name, String dest_name)
			throws UnsupportedEncodingException, FileNotFoundException, IOException {

		System.out.println(" ---------- convertSimpleFiles "+original_name);

		Reader reader = new BufferedReader(new InputStreamReader(new FileInputStream(output_dir+File.separator+original_name), "UTF8"));
		CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader()); 

		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output_dir+File.separator+dest_name, false), "UTF8"));
		CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(csvParser.getHeaderNames().toArray(new String[] {})));

		List<String> header = csvParser.getHeaderNames();
		System.out.println(header);
		Set<String> to_shorten = new HashSet<>();
		to_shorten.add("tweetId");to_shorten.add("userId");to_shorten.add("createdAt");
		to_shorten.add("retweet");to_shorten.add("reply");to_shorten.add("quote");

		int i=0;

		for(CSVRecord record : csvParser) {

			if(i % 10_000 == 0)
				System.out.println(i+" "+new Date());
			i++;

			List<Object> rec = new ArrayList<>();
			for(int j=0;j<record.size();j++) {
				//				System.out.println(header.get(j));
				if(to_shorten.contains(header.get(j)))
					rec.add(Long.toString(Long.parseLong(record.get(header.get(j))),Character.MAX_RADIX));
				else
					rec.add(record.get(header.get(j)));
			}


			csvPrinter.printRecord(rec);
		}
		csvPrinter.close();
		writer.close();

		csvParser.close();
		reader.close();
	}

	public static void statisticsUsers(String output_dir) throws IOException {

		BufferedReader reader = Files.newBufferedReader(Paths.get(output_dir+File.separator+"_users_tweets_type.csv"));
		CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader()); 

		long total = 0;
		long total_square = 0;

		IntList list = new IntArrayList();
		for(CSVRecord r : csvParser) {
			int c = Integer.parseInt(r.get("total"));
			list.add(c);
			total += c;
			total_square += c*c;
		}

		csvParser.close();
		reader.close();

		System.out.println(total+" "+total_square+" "+list.size());

		System.out.println(list.stream().collect(Collectors.summarizingInt(Integer::intValue)));
		System.out.println(Math.sqrt(total_square/(float)list.size() - ((total/(float)list.size())*(total/(float)list.size()))));
	}

	//for the sake of space!
	private static void mergeTweetCreatedAtUser(String output_dir) throws IOException {

		System.out.println("--------------------- mergeTweetCreatedAtUser");

		Long2LongMap tweet_user = new Long2LongOpenHashMap(108_000_000);
		Reader reader = Files.newBufferedReader(Paths.get(output_dir+File.separator+"tweets_user.csv"));
		CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader()); 
		int k = 0;
		for(CSVRecord record : csvParser) {
			if(k % 100_000 == 0)
				System.out.println(k+" "+new Date());
			k++;
			tweet_user.put(Long.parseLong(record.get("tweetId")), Long.parseLong(record.get("userId")));
		}

		csvParser.close();
		reader.close();

		BufferedWriter writer = Files.newBufferedWriter(Paths.get(output_dir+File.separator+"_tweetd_user_createdAt.csv"));
		CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader("tweetId","userId","createdAt","place_fullName","place_country"));

		reader = Files.newBufferedReader(Paths.get(output_dir+File.separator+"_tweets_created_place.csv")); //de acá saco tweet id de las relaciones
		csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader()); 

		for(CSVRecord record : csvParser) {

			long tid = Long.parseLong(record.get("tweetId"));

			if(record.size() > 2)
				csvPrinter.printRecord(new Object[] {tid,tweet_user.remove(tid),record.get("createdAt"),record.get("place_fullName"),record.get("place_country")});
			else
				csvPrinter.printRecord(new Object[] {tid,tweet_user.remove(tid),record.get("createdAt"),"",""});

		}

		csvParser.close();
		reader.close();

		csvPrinter.close();
		writer.close();

	}

	private static void mergeTableUserRelations(String output_dir) throws IOException {
		System.out.println("--------------------- mergeTableUserRelations");

		BufferedWriter writer = Files.newBufferedWriter(Paths.get(output_dir+File.separator+"_users_graph_merged.csv"));
		CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader("userId","relation","ids"));

		Reader reader = Files.newBufferedReader(Paths.get(output_dir+File.separator+"_users_graph.csv"));
		CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader()); 

		Map<Long,LongSet[]> user_relations = new HashMap<>(11_000_000,0.9f); //< user, [retweet,reply,quoted] >  --> reply >> quoted

		int j = 0;

		for(CSVRecord record : csvParser) {

			if(j % 100_000 == 0)
				System.out.println(j+" "+new Date());
			j++;

			if(record.get("relation").equals("4"))
				csvPrinter.printRecord(record);
			else {

				long tid = Long.parseLong(record.get("userId"));
				LongSet[] aux = user_relations.get(tid);
				if(aux == null) {
					aux = new LongSet[3];
					user_relations.put(tid, aux);
				}
				int type = Integer.parseInt(record.get("relation")) - 1; 
				if(aux[type] == null)
					aux[type] = new LongOpenHashSet(10);

				String [] sp = record.get("ids").split(" ");
				for(String s : sp)
					if(s.length() > 0)
						aux[type].add(Long.parseLong(s));
			}

		}

		csvParser.close();
		reader.close();

		for(Entry<Long,LongSet []> e : user_relations.entrySet()) {
			LongSet [] ids = e.getValue();
			for(int i=0;i<ids.length;i++) {
				if(ids[i] == null || ids[i].size() == 0)
					continue;

				csvPrinter.printRecord(new Object[] {e.getKey(),
						i+1,
						ids[i].stream().map(it -> Long.toString(it)).collect(Collectors.joining(" "))
				});
			}
		}

		csvPrinter.close();
		writer.close();


	}

	public static void main(String[] args) throws IOException {

		System.out.println(ProcessOutputs.class.getCanonicalName());		

		String output_dir = args[0];

		//		transformTweetType(output_dir);

		filterRT(output_dir);
		processDates(output_dir);
		createTableTweetCreatedAtPlace(output_dir);
		createTableTweetMediaURL(output_dir);
		createTableUserTweets(output_dir);
		filterUsersWithTweets(output_dir);

		createTableUserRelations(output_dir);
		mergeTableUserRelations(output_dir);

		mergeTweetCreatedAtUser(output_dir);
		filterHashtags(output_dir);
		createTableTweetHashtags(output_dir); 

		tweetsInQuery(output_dir);

		statisticsUsers(output_dir);
		System.out.println(getVerifiedUsers(output_dir+File.separator+"_users_filtered.csv").size());

		fileLongConversion(output_dir);

		//		getTweetsVerifiedUsers(output_dir,5);
		//		getTweetsVerifiedUsers(output_dir,10);
		//		getTweetsVerifiedUsers(output_dir,20);
		//		getTweetsVerifiedUsers(output_dir,25);

	}

}
