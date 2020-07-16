package edu.isistan.fakenews.webcrawler

import org.opencv.core.Core
import org.opencv.imgcodecs.Imgcodecs
import org.opencv.core.Mat
import org.opencv.imgproc.Imgproc
import org.opencv.core.MatOfPoint
import org.opencv.core.Point
import org.opencv.core.MatOfPoint2f
import org.opencv.core.Scalar
import java.util.Collections
import java.awt.image.BufferedImage
import net.sourceforge.tess4j.Tesseract
import net.sourceforge.tess4j.util.LoadLibs
import java.io.File
import org.jsoup.Jsoup
import org.jsoup.nodes.Element
import java.util.regex.Pattern
import javax.imageio.ImageIO

import edu.isistan.fakenews.storage.MongoDBStorage
import edu.isistan.fakenews.storage.Tweet
import org.bson.types.ObjectId
import java.util.stream.Collector
import java.util.stream.Collectors

	val GREEN = Scalar(0.0,255.0,0.0)
	val BLACK = Scalar(0.0,0.0,0.0)

class ScreenshotProcessing

fun init(){

	System.setProperty("java.library.path", "\\opencv\\build\\java\\x64")

	val sysPathsField = ClassLoader::class.java.getDeclaredField("sys_paths")

	sysPathsField.setAccessible(true)
	sysPathsField.set(null, null)

	System.loadLibrary(Core.NATIVE_LIBRARY_NAME)

}

fun processImage(image : BufferedImage, tweet : Tweet<ObjectId>) : BufferedImage{

	val imgSource = image.toMat()

			val aux = Mat()
			Imgproc.cvtColor(imgSource, aux, Imgproc.COLOR_BGR2GRAY)

//	Imgcodecs.imwrite("modified_grey.png", aux);
	
			Imgproc.threshold(aux,aux,254.0,255.0,Imgproc.THRESH_BINARY) //todo lo que est� por fuera del recuadro queda negro.

//	Imgcodecs.imwrite("modified_bw.png", aux);
	
			val hierarchy = Mat()
			var contours = ArrayList<MatOfPoint>()
			Imgproc.findContours(aux, contours, hierarchy, Imgproc.RETR_TREE, Imgproc.CHAIN_APPROX_SIMPLE)

			val all_points = ArrayList<Point>();

	contours.filterIndexed{ i,m -> hierarchy.get(0,i)[3].toInt() == -1 && Imgproc.contourArea(m) >= 1000}
	.forEach{

		val matOfPoint2f = MatOfPoint2f()
				val approxCurve = MatOfPoint2f()

				matOfPoint2f.fromList(it.toList())

				Imgproc.approxPolyDP(matOfPoint2f, approxCurve, Imgproc.arcLength(matOfPoint2f, true) * 0.02, true)
				val total = approxCurve.total()

				if (total >= 4 && total <= 7) {
					val cos = ArrayList<Double>()
							val points = approxCurve.toArray()

							for(j in 2..total)
								cos.add(angle(points[(j % total).toInt()], points[(j - 2).toInt()], points[(j - 1).toInt()]));
									val minCos = cos.min();
									//			val maxCos = cos.max();

									val isRect = (minCos!! >= -0.1)// && maxCos <= 0.3;

											LOGGER.debug("Detecting forms {} {} {}",Imgproc.contourArea(it),total,cos);

									if (isRect) 
										all_points.addAll(it.toList());


				}
	}

	val rect = Imgproc.boundingRect(MatOfPoint(*all_points.toArray(arrayOf<Point>())));

	val thread = Mat(imgSource,rect)
			Core.copyMakeBorder(thread, thread, 5, 5, 5, 5, Core.BORDER_CONSTANT, BLACK) //5

			//	Imgcodecs.imwrite("modified_thread2.png", thread); //hasta ac� me quedo solo con el sector de tweets
//				Imgcodecs.imwrite("modified_rects.png", imgSource);
//							Imgcodecs.imwrite("modified_bw.png", aux);

			//this is for removing circles
			val circle_areas = HashSet<Double>()
			val circle_indexes = HashSet<Int>()
	
			for((i,m) in contours.withIndex()){

				val contourArea = Imgproc.contourArea(m)
						val perimeter = Imgproc.arcLength(MatOfPoint2f(*m.toArray()), true)
						var circularity = 4.0*contourArea*Math.PI/(perimeter*perimeter)
						circularity = 1 - circularity
						if(circularity < 0.10) { 
							circle_areas.add(contourArea)
							circle_indexes.add(i)
						}
			}


	val circle_areas_sorted = ArrayList<Double>(circle_areas)
			circle_areas_sorted.sortWith(Collections.reverseOrder())

			LOGGER.debug("Sorted circle areas: {}",circle_areas_sorted)

			var min_area : Double
			if(tweet.favoriteCount > 0 || tweet.retweetCount > 0)
				min_area = circle_areas_sorted.get(1)
				else
					min_area = circle_areas_sorted.get(0)

					contours.forEachIndexed{
					i,m -> run { 
						if(circle_indexes.contains(i) && Imgproc.contourArea(m) >= min_area)
							//				Imgproc.drawContours(imgSource, contours, i, Scalar(0.0,Math.abs(255-Imgproc.contourArea(m)/30).toDouble(),0.0), -1);
							Imgproc.drawContours(imgSource, contours, i, BLACK, -1);
					}
				}

			val thread1 = Mat(imgSource,rect)
			Core.copyMakeBorder(thread1, thread1, 5, 5, 5, 5, Core.BORDER_CONSTANT, BLACK)

			return thread1.toBufferedImage()
}

fun getTweetRectangle(image : BufferedImage) : BufferedImage{

	val imgSource = image.toMat()

			val aux = Mat()
			Imgproc.cvtColor(imgSource, aux, Imgproc.COLOR_BGR2GRAY)

//	Imgcodecs.imwrite("modified_grey.png", aux);
	
			Imgproc.threshold(aux,aux,254.0,255.0,Imgproc.THRESH_BINARY) //todo lo que est� por fuera del recuadro queda negro.

//	Imgcodecs.imwrite("modified_bw.png", aux);
	
			val hierarchy = Mat()
			var contours = ArrayList<MatOfPoint>()
			Imgproc.findContours(aux, contours, hierarchy, Imgproc.RETR_TREE, Imgproc.CHAIN_APPROX_SIMPLE)

			val all_points = ArrayList<Point>();

	contours.filterIndexed{ i,m -> hierarchy.get(0,i)[3].toInt() == -1 && Imgproc.contourArea(m) >= 1000}
	.forEach{

		val matOfPoint2f = MatOfPoint2f()
				val approxCurve = MatOfPoint2f()

				matOfPoint2f.fromList(it.toList())

				Imgproc.approxPolyDP(matOfPoint2f, approxCurve, Imgproc.arcLength(matOfPoint2f, true) * 0.02, true)
				val total = approxCurve.total()

				if (total >= 4 && total <= 7) {
					val cos = ArrayList<Double>()
							val points = approxCurve.toArray()

							for(j in 2..total)
								cos.add(angle(points[(j % total).toInt()], points[(j - 2).toInt()], points[(j - 1).toInt()]));
									val minCos = cos.min();
									//			val maxCos = cos.max();

									val isRect = (minCos!! >= -0.1)// && maxCos <= 0.3;

											LOGGER.debug("Detecting forms {} {} {}",Imgproc.contourArea(it),total,cos);

									if (isRect) 
										all_points.addAll(it.toList());
				}
	}

	val rect = Imgproc.boundingRect(MatOfPoint(*all_points.toArray(arrayOf<Point>())));

	val thread = Mat(imgSource,rect)
			Core.copyMakeBorder(thread, thread, 5, 5, 5, 5, Core.BORDER_CONSTANT, BLACK) //5
	
	return thread.toBufferedImage()
}


private fun angle(pt1 : Point, pt2 : Point, pt0 : Point) : Double{
	val dx1 = pt1.x - pt0.x;
	val dy1 = pt1.y - pt0.y;
	val dx2 = pt2.x - pt0.x;
	val dy2 = pt2.y - pt0.y;
	return (dx1*dx2 + dy1*dy2)/Math.sqrt((dx1*dx1 + dy1*dy1)*(dx2*dx2 + dy2*dy2) + 1e-10);
}

fun obfuscateImage(image : BufferedImage, tweet : Tweet<ObjectId>, screenname : String) : BufferedImage{

	val tesseract = Tesseract() 
			//In case you don't have your own tessdata, let it also be extracted for you
			val tessDataFolder = LoadLibs.extractTessResources("tessdata")
			//Set the tessdata path
			tesseract.setDatapath(tessDataFolder.getAbsolutePath())
			tesseract.setHocr(true); //para que muestre m�s info del texto

	val bboxes = HashSet<Pair<MutableList<String>,Scalar>>()

			val imgSource  = image.toMat()

			val grey_image = Mat()

			var text : String? = null
			if(tweet.quotedStatusId != -1L)
				text = tweet.text.substring(0, tweet.text.lastIndexOf("https"))


				Imgproc.cvtColor(imgSource, grey_image, Imgproc.COLOR_BGR2GRAY) //processing a grey scale version of the image
				bboxes.addAll(searchWords(tesseract,grey_image,tweet.quotedStatusId != -1L,text,screenname.toLowerCase()))

				Imgproc.threshold(grey_image,grey_image,250.0,255.0,Imgproc.THRESH_BINARY) //processing a b&w version of the image
				bboxes.addAll(searchWords(tesseract,grey_image,tweet.quotedStatusId != -1L,text,screenname.toLowerCase()))

				return 	maskWords(bboxes,imgSource).toBufferedImage()
}

private fun searchWords(tesseract : Tesseract, image : Mat,quoted : Boolean,tweetText : String?, screenname : String) : HashSet<Pair<MutableList<String>,Scalar>>{

	val text = tesseract.doOCR(image.toBufferedImage())
	val doc = Jsoup.parse(text)

	val previous_line = ArrayList<String>()
	val previous_positions = ArrayList<MutableList<String>>()

	val bboxes = HashSet<Pair<MutableList<String>,Scalar>>()

	val closest_line = ArrayList<MutableList<String>>()
	var min_distance = 0
	if(quoted)
		min_distance = tweetText!!.length + 2

	doc.getElementsByClass("ocr_line")?.forEachIndexed{

	i,line_w -> run {
		
		val bboxes_line = HashSet<Pair<MutableList<String>,Scalar>>()
		
		//has some strange behaviour with the limits
//		val pos_line = line_w.attr("title").substring(5,line_w.attr("title").indexOf(";")).split(" ") //para hacer mejor lo de las posiciones
				
//		line_w.getElementsByClass("ocrx_word")?.forEach{
//			print(it.text()+" ")
//		}
//		println("")

		if(i == 0){ //the first line is always added

			line_w.getElementsByClass("ocrx_word")?.forEach{

				if(it.text().toLowerCase().equals(screenname) || it.text().startsWith("@"))
					bboxes_line.add(Pair(getPosition(it),BLACK));
					
			}
			
		}else{ //if it is not the first line

			var words_with_at = 0

			val line = ArrayList<String>()
			val pos = ArrayList<MutableList<String>>()

			val words = line_w.getElementsByClass("ocrx_word")
			
			if(words?.size == 1 && words.get(0).text().toLowerCase().equals(screenname))
				bboxes_line.add(Pair(getPosition(words.get(0)),BLACK))
			
				words.forEachIndexed{
					j,ww -> run{

						val word = ww.text()
						line.add(word)
					    val bbox_sp = getPosition(ww)
						pos.add(bbox_sp);

						if(word.startsWith("@") && word.replace("@","").length > 0) { //encontr� algo que tengo que procesar!
							words_with_at++;
							bboxes_line.add(Pair(bbox_sp,BLACK)) //petroleo
							LOGGER.debug("Words starting with @ {} {}",word,bbox_sp)
						}

//						if(j < 2 && word.toLowerCase().equals("replying")){ //we need to add previous line up to the @
//							
//							println(line+" -- "+previous_line)
//							
//							var i = 0
//							while(i < previous_line.size && !previous_line.get(i).startsWith("@")) 
//								i++;
//
//							if(i < (previous_line.size - 1)){
////								bboxes.addAll(previous_positions.subList(0,i))
//								for(k in 0 until i)
//									bboxes.add(Pair(previous_positions.get(k),Scalar(150.0,150.0,0.0))) //mostaza
////								println(previous_line.subList(0,i))
//							}
//									
//						}
					
					}
				}//line has been processed!

				if(words_with_at == 0) { //no mention found
					previous_line.clear();
					previous_line.addAll(line);
					previous_positions.clear();
					previous_positions.addAll(pos);
				}
				else{ //this was to check the previous line in case we only find an @ in the line. That is done in another place, but we still need to check the previous line for another special case
				
					if(line.size == words_with_at
						&& line.find { Pattern.matches(".*[\\p{Punct}&&[^_@]].*",it) } == null) { //need to add previous line (?)

						//re check the previous line
						LOGGER.debug("Line: {} Previous Line {}",line,previous_line);

						var p = previous_positions.size
						if(previous_positions.size-2 > 1)
							p = p-2
						
						for(k in 0 until p)
							bboxes_line.add(Pair(previous_positions.get(k),BLACK))

//							bboxes.addAll(previous_positions.subList(0,previous_positions.size));
						
					}else { //we add until we find a @
						
						var i = 0
						while(i < line.size && !line.get(i).startsWith("@")) 
							i++;

						if(i < (line.size - 1) && line.get(i+1).trim().equals("-")){
							for(k in 0 until i)
								bboxes_line.add(Pair(pos.get(k),BLACK)) //bord�
//							bboxes.addAll(pos.subList(0,i))
						}
							
							
						if(quoted){ //the quoted tweet has a special line with the @ and the name of the user that publish the original tweet.
							//Always the previous line contains the text of the tweet, or at least part of it. Hence, instead of playing with the forms, we check the edit distance of the last line
							//with the text of the tweet up to the url of the quoted tweet. We keep the line with the smallest distance
							val edit_distance = editDistance(tweetText!!,previous_line.stream().collect(Collectors.joining(" ")))
							if(edit_distance < min_distance){
								min_distance = edit_distance
								closest_line.clear()
								closest_line.addAll(pos)
							}
						}

						previous_line.clear()
						previous_line.addAll(line)

						previous_positions.clear()
						previous_positions.addAll(pos)

					}
					}		
				}
		if(bboxes_line.size > 0)
			bboxes.addAll(fixLine(bboxes_line))
	

		
		}
	}
	if(closest_line.size > 0){
//		bboxes.addAll(closest_line)
		val bboxes_line = HashSet<Pair<MutableList<String>,Scalar>>()
		closest_line.forEach{
			bboxes_line.add(Pair(it,BLACK))
		}
		bboxes.addAll(fixLine(bboxes_line))
	}
		
				

	return bboxes
}

	private fun fixLine(bboxes_line : HashSet<Pair<MutableList<String>,Scalar>>) : HashSet<Pair<MutableList<String>,Scalar>>{
//		val new_line = HashSet<Pair<MutableList<String>,Scalar>>()
		
		val up = HashSet<Int>()
		val down = HashSet<Int>()
			
		bboxes_line.forEach{
			if(it.component1()[1].toInt() != 0){
				up.add(it.component1()[1].toInt())
				down.add(it.component1()[3].toInt())
			}

		}
		
		val min_up = up.min().toString()
		val max_down = down.max().toString()
		
		bboxes_line.forEach{
			it.component1()[1] = min_up
			it.component1()[3] = max_down
		}
		
		return bboxes_line
	}

	private fun getPosition(element : Element) : MutableList<String>{
		
		var bbox : String = element.attr("title")
				bbox = bbox.substring(5,bbox.indexOf(";"))
				var bbox_sp = bbox.split(" ").toMutableList()
//				bbox_sp[1] = pos_line[1]
//				bbox_sp[3] = pos_line[3]
		
		return bbox_sp
	}

	private fun maskWords(bboxes : HashSet<Pair<MutableList<String>,Scalar>>, imgSource : Mat) : Mat{

		
		
		for(sp in bboxes) {
			if(sp.component1()[0].toInt() != 0 && sp.component1()[2].toInt() != imgSource.width()){
				val points = ArrayList<Point>();
			points.add(Point(sp.component1()[0].toDouble()-3, sp.component1()[1].toDouble()));
			points.add(Point(sp.component1()[2].toDouble()+3, sp.component1()[1].toDouble()));
			points.add(Point(sp.component1()[2].toDouble()+3, sp.component1()[3].toDouble()));
			points.add(Point(sp.component1()[0].toDouble()-3, sp.component1()[3].toDouble()));

			val matPt = MatOfPoint()
					matPt.fromList(points);
			val ppt = ArrayList<MatOfPoint>()
					ppt.add(matPt)

//					Imgproc.fillPoly(imgSource,ppt,Scalar(255.0,0.0,0.0),1,0,Point(0.0,0.0));
					Imgproc.fillPoly(imgSource,ppt,sp.component2(),1,0,Point(0.0,0.0));
		}
			}
			
			

		return imgSource
	}

	fun editDistance(word1 : String, word2 : String) : Int{
		val len1 = word1.length
		val len2 = word2.length

		// len1+1, len2+1, because finally return dp[len1][len2]
 	    val dp = Array(len1+1){IntArray(len2+1)}

		for(i in 0..len1)
			dp[i][0] = i

		for(j in 0..len2)
			dp[0][j] = j

		for(i in 0 until len1){
			val c1 = word1.get(i)
			for(j in 0 until len2){
				val c2 = word2.get(j)
					if(c1 == c2)
						dp[i + 1][j + 1] = dp[i][j]
					else{
						val replace = dp[i][j] + 1
						val insert = dp[i][j + 1] + 1
						val delete = dp[i + 1][j] + 1
						dp[i + 1][j + 1] = Math.min(Math.min(replace,insert),delete)
					}
			}
		}

		return dp[len1][len2];
	}


	fun main(){
		init()

//		DEBUG_DB=true
//		val storage = MongoDBStorage()
//		val tweet : Long
////		tweet = 1205245702512754688L
////		tweet =1200096974541856776L
//		tweet = 1191792251573288966L
//		val tw = storage.findTweet(tweet)!!
//		val sn = storage.findUser(tw.userId!!)!!.name
//
//		val processed =	processImage(ImageIO.read(File(tweet.toString()+".png")),tw) 
//		
//		ImageIO.write(processed , "png", File("processed_opencv.png") );
//
//		val obs = obfuscateImage(processed,tw,sn) 
//		ImageIO.write(obs , "png", File("obs_tesseract.png") );
		
		
		val processed =	getTweetRectangle(ImageIO.read(File("test.png")))
		ImageIO.write(processed , "png", File("rect.png"))
	}