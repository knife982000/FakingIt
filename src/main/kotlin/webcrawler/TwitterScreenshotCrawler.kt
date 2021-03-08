package edu.isistan.fakenews.webcrawler

import com.google.gson.Gson
import edu.isistan.fakenews.scrapper.windowInnerHeight
import edu.isistan.fakenews.scrapper.windowScrollY
import edu.isistan.fakenews.storage.MongoDBStorage
import org.openqa.selenium.Dimension
import java.awt.image.BufferedImage
import org.openqa.selenium.JavascriptExecutor
import org.openqa.selenium.support.ui.WebDriverWait
import org.openqa.selenium.TakesScreenshot
import org.openqa.selenium.OutputType
import java.io.ByteArrayInputStream
import javax.imageio.ImageIO
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.ObjectInput
import java.nio.file.Files
import java.nio.file.Paths
import edu.isistan.fakenews.scrapper.windowScrollTo

class TwitterScreenshotCrawler(storage: MongoDBStorage, format: String = "png"): ScreenshotCrawler(storage, format) {

	override fun takeScreenshot(url: String): BufferedImage {
		this.driver.get(url)
		this.driver.manage().window().size = Dimension(System.getProperty("screenshot.height")?.toIntOrNull()?: 800,
				System.getProperty("screenshot.width")?.toIntOrNull()?: 600)
		WebDriverWait(this.driver, 10)
		val javascript = driver as JavascriptExecutor
		val screenshot = driver as TakesScreenshot
		val scroll = windowInnerHeight(javascript) / 2

		var lastTop = -1
		val pages = mutableListOf<BufferedImage>()
		var positions = mutableListOf<Int>()
		Thread.sleep(2500)//Wait to load... it is horrible but it kind of work!
		while (lastTop != windowScrollY(javascript).toInt()) {
			javascript.executeScript("""
					if (document.getElementsByClassName("css-18t94o4 css-1dbjc4n r-1777fci r-1jayybb r-o7ynqc r-1j63xyz r-13qz1uu").length == 1) {
						document.getElementsByClassName("css-18t94o4 css-1dbjc4n r-1777fci r-1jayybb r-o7ynqc r-1j63xyz r-13qz1uu")[0].click()
					}
					""".trimIndent())
					javascript.executeScript("""
							if (document.getElementsByClassName("css-18t94o4 css-1dbjc4n r-1777fci r-1jayybb r-1ny4l3l r-o7ynqc r-6416eg r-13qz1uu").length == 1) {
								document.getElementsByClassName("css-18t94o4 css-1dbjc4n r-1777fci r-1jayybb r-1ny4l3l r-o7ynqc r-6416eg r-13qz1uu")[0].click()
							}
							""".trimIndent())
							Thread.sleep(1000)//Wait to load... it is horrible but it kind of work!
							val pos = windowScrollY(javascript).toInt()
							pages.add(ImageIO.read(ByteArrayInputStream(screenshot.getScreenshotAs(OutputType.BYTES))))
							positions.add(pos)
							lastTop = pos
							javascript.executeScript("window.scrollTo(0, ${lastTop + scroll})")
		}

		val pixelSize = pages[0].height.toDouble() / windowInnerHeight(javascript)
				return stitchScreenshots(pages, positions.map { (it * pixelSize).toInt() })
	}


}

class TwitterObfuscatedScreenshotCrawler(storage: MongoDBStorage, format: String = "png"): ScreenshotCrawler(storage, format) {

	val obfuscatorJS = run {
		//		val resourceFile = File(Paths.get(ClassLoader.getSystemClassLoader().getResource("twitterObfuscator.js").toURI()).toString())
		val resourceFile = File("twitterObfuscator.js")
				Files.readAllLines(resourceFile.toPath()).reduce {
			a, b -> "$a\n$b"
		}
	}

	val varsObfuscatorJS = run {
		//		val resourceFile = File(ClassLoader.getSystemClassLoader().getResource("twitterObfscatorVariables.js").file)
		//		val resourceFile = File(Paths.get(ClassLoader.getSystemClassLoader().getResource("twitterObfscatorVariables.js").toURI()).toString())
		val resourceFile = File("twitterObfscatorVariables.js")
				Files.readAllLines(resourceFile.toPath()).reduce {
			a, b -> "$a\n$b"
		}
	}

	override fun takeScreenshot(url: String): BufferedImage {
		val gson = Gson()
				this.driver.get(url)
				//this.driver.manage().window().fullscreen()
				this.driver.manage().window().size = Dimension(System.getProperty("screenshot.height")?.toIntOrNull()?: 800,
						System.getProperty("screenshot.width")?.toIntOrNull()?: 600)
				WebDriverWait(this.driver, 10)
				val javascript = driver as JavascriptExecutor
				val screenshot = driver as TakesScreenshot
				val scroll = windowInnerHeight(javascript) / 2

				var lastTop = -1
				val pages = mutableListOf<BufferedImage>()
				val positions = mutableListOf<Int>()
				Thread.sleep(5000)//Wait to load... it is horrible but it kind of work!
				var data = javascript.executeScript(this.varsObfuscatorJS)
				while (lastTop != windowScrollY(javascript).toInt()) {
					javascript.executeScript("""
							if (document.getElementsByClassName("css-18t94o4 css-1dbjc4n r-1777fci r-1jayybb r-o7ynqc r-1j63xyz r-13qz1uu").length == 1) {
								document.getElementsByClassName("css-18t94o4 css-1dbjc4n r-1777fci r-1jayybb r-o7ynqc r-1j63xyz r-13qz1uu")[0].click()
							}
							""".trimIndent())
							javascript.executeScript("""
									if (document.getElementsByClassName("css-18t94o4 css-1dbjc4n r-1777fci r-1jayybb r-1ny4l3l r-o7ynqc r-6416eg r-13qz1uu").length == 1) {
										document.getElementsByClassName("css-18t94o4 css-1dbjc4n r-1777fci r-1jayybb r-1ny4l3l r-o7ynqc r-6416eg r-13qz1uu")[0].click()
									}
									""".trimIndent())
					
						javascript.executeScript("""
									if (document.getElementsByClassName("css-18t94o4 css-1dbjc4n r-1niwhzg r-42olwf r-sdzlij r-1phboty r-rs99b7 r-1w2pmg r-1vsu8ta r-aj3cln r-1ny4l3l r-utggzx r-o7ynqc r-6416eg r-lrvibr").length == 1) {
										document.getElementsByClassName("css-18t94o4 css-1dbjc4n r-1niwhzg r-42olwf r-sdzlij r-1phboty r-rs99b7 r-1w2pmg r-1vsu8ta r-aj3cln r-1ny4l3l r-utggzx r-o7ynqc r-6416eg r-lrvibr")[0].click()
									}
							""".trimIndent())
									Thread.sleep(1000)//Wait to load... it is horrible but it kind of work!

									val pos = windowScrollY(javascript).toInt()
									data = javascript.executeScript("data = ${gson.toJson(data)}\n"+this.obfuscatorJS)


									pages.add(ImageIO.read(ByteArrayInputStream(screenshot.getScreenshotAs(OutputType.BYTES))))
									positions.add(pos)
									lastTop = pos
									javascript.executeScript("window.scrollTo(0, ${lastTop + scroll})")
				}

		val pixelSize = pages[0].height.toDouble() / windowInnerHeight(javascript)
				return stitchScreenshots(pages, positions.map { (it * pixelSize).toInt() })
	}

	fun run(tweetIds : ArrayList<Long>){
		init()
		LOGGER.info("Taking obfuscated tweet screenshots")

		//		super.run(tweetIds) 

		tweetIds.filter{this.storage.findTweetScreenshot(it) == null}.forEach{

			val tweet = this.storage.findTweet(it)
					if(tweet != null){
//						val user = this.storage.findUser(tweet.userId)
//								val url = "https://twitter.com/${user!!.userId}/status/$it"
						val url = "https://twitter.com/any/status/$it" 
								val screenshot = this.takeScreenshot(url)
								val bytes = ByteArrayOutputStream()
								ImageIO.write(screenshot.ensureOpaque(), this.format, bytes)

								//					val rectangleImage = getTweetRectangle(screenshot)
								//					val bytesObs = ByteArrayOutputStream()
								//					ImageIO.write(rectangleImage.ensureOpaque(), this.format, bytesObs)
								//			
								//					this.storage.storeTweetScreenshot(it.toString(), bytesObs.toByteArray(), readInverseMimeType()[".${this.format}"] ?:
								//						error("Invalid extension .${this.format}"))

								this.storage.storeTweetScreenshot(it.toString(), bytes.toByteArray(), readInverseMimeType()[".${this.format}"] ?:
									error("Invalid extension .${this.format}"))	
					}

		}
	}
}

class TwitterScreenshotAndObfuscatedCrawler(storage: MongoDBStorage, format: String = "png"): ScreenshotCrawler(storage, format) {

	val obfuscatorJS = run {
		//		val resourceFile = File(Paths.get(ClassLoader.getSystemClassLoader().getResource("twitterObfuscator.js").toURI()).toString())
		val resourceFile = File("twitterObfuscator.js")
//		val resourceFile = File(Thread.currentThread().getContextClassLoader().getResourceAsStream("/twitterObfuscator.js").toString())
				Files.readAllLines(resourceFile.toPath()).reduce {
			a, b -> "$a\n$b"
		}
	}

	val varsObfuscatorJS = run {
		//		val resourceFile = File(ClassLoader.getSystemClassLoader().getResource("twitterObfscatorVariables.js").file)
		//		val resourceFile = File(Paths.get(ClassLoader.getSystemClassLoader().getResource("twitterObfscatorVariables.js").toURI()).toString())
		val resourceFile = File("twitterObfscatorVariables.js")
				Files.readAllLines(resourceFile.toPath()).reduce {
			a, b -> "$a\n$b"
		}
	}
	
	override fun takeScreenshot(url: String): BufferedImage {
		//never going to get used
		return BufferedImage(0, 0, BufferedImage.TYPE_INT_RGB)
	}

	fun takeScreenshots(url: String): Pair<BufferedImage, BufferedImage> {
		val gson = Gson()
				this.driver.get(url)
				//this.driver.manage().window().fullscreen()
				this.driver.manage().window().size = Dimension(System.getProperty("screenshot.height")?.toIntOrNull()?: 800,
						System.getProperty("screenshot.width")?.toIntOrNull()?: 600)
				WebDriverWait(this.driver, 10)
				val javascript = driver as JavascriptExecutor
				val screenshot = driver as TakesScreenshot
				val scroll = windowInnerHeight(javascript) / 2

				var lastTop = -1
				val pages = mutableListOf<BufferedImage>()
				val positions = mutableListOf<Int>()
				Thread.sleep(1000)//Wait to load... it is horrible but it kind of work!
				while (lastTop != windowScrollY(javascript).toInt()) {
					javascript.executeScript("""
							if (document.getElementsByClassName("css-18t94o4 css-1dbjc4n r-1777fci r-1jayybb r-o7ynqc r-1j63xyz r-13qz1uu").length == 1) {
								document.getElementsByClassName("css-18t94o4 css-1dbjc4n r-1777fci r-1jayybb r-o7ynqc r-1j63xyz r-13qz1uu")[0].click()
							}
							""".trimIndent())
							javascript.executeScript("""
									if (document.getElementsByClassName("css-18t94o4 css-1dbjc4n r-1777fci r-1jayybb r-1ny4l3l r-o7ynqc r-6416eg r-13qz1uu").length == 1) {
										document.getElementsByClassName("css-18t94o4 css-1dbjc4n r-1777fci r-1jayybb r-1ny4l3l r-o7ynqc r-6416eg r-13qz1uu")[0].click()
									}
									""".trimIndent())

							javascript.executeScript("""
									if (document.getElementsByClassName("css-18t94o4 css-1dbjc4n r-1niwhzg r-42olwf r-sdzlij r-1phboty r-rs99b7 r-1w2pmg r-1vsu8ta r-aj3cln r-1ny4l3l r-utggzx r-o7ynqc r-6416eg r-lrvibr").length == 1) {
										document.getElementsByClassName("css-18t94o4 css-1dbjc4n r-1niwhzg r-42olwf r-sdzlij r-1phboty r-rs99b7 r-1w2pmg r-1vsu8ta r-aj3cln r-1ny4l3l r-utggzx r-o7ynqc r-6416eg r-lrvibr")[0].click()
									}
							""".trimIndent())
				
									Thread.sleep(1000)//Wait to load... it is horrible but it kind of work!

									val pos = windowScrollY(javascript).toInt()

									pages.add(ImageIO.read(ByteArrayInputStream(screenshot.getScreenshotAs(OutputType.BYTES))))
									positions.add(pos)
									lastTop = pos
									javascript.executeScript("window.scrollTo(0, ${lastTop + scroll})")
				}
		
		val pixelSize = pages[0].height.toDouble() / windowInnerHeight(javascript)
		val screenshot_normal = stitchScreenshots(pages, positions.map { (it * pixelSize).toInt() })
		
		windowScrollTo(javascript,0,positions.first()) // we scroll to the position of the first image
		lastTop = -1
		positions.clear()
		pages.clear()
		
		var data = javascript.executeScript(this.varsObfuscatorJS)
		while (lastTop != windowScrollY(javascript).toInt()) {

			val pos = windowScrollY(javascript).toInt()
			data = javascript.executeScript("data = ${gson.toJson(data)}\n"+this.obfuscatorJS)

			pages.add(ImageIO.read(ByteArrayInputStream(screenshot.getScreenshotAs(OutputType.BYTES))))
			positions.add(pos)
			lastTop = pos
			javascript.executeScript("window.scrollTo(0, ${lastTop + scroll})")
		}

		val screenshot_obfuscated = stitchScreenshots(pages, positions.map { (it * pixelSize).toInt() })
		return Pair(screenshot_normal,screenshot_obfuscated)
	}

	fun run(tweetIds : ArrayList<Long>){
		init()
		LOGGER.info("Taking normal and obfuscated tweet screenshots")

		//we have the normal but not the other one. We only need the obfuscator, so we can create an instance of the other and thats's it
		val obfuscator = TwitterObfuscatedScreenshotCrawler(this.storage)
		tweetIds.filter{this.storage.findScreenshot(it) != null && this.storage.findTweetScreenshot(it) == null}.forEach{

			val tweet = this.storage.findTweet(it)
					if(tweet != null){
//						val user = this.storage.findUser(tweet.userId)
//								val url = "https://twitter.com/${user!!.userId}/status/$it"
					
								val url = "https://twitter.com/any/status/$it" 
								val screenshot = obfuscator.takeScreenshot(url)
								val bytes = ByteArrayOutputStream()
								ImageIO.write(screenshot.ensureOpaque(), this.format, bytes)

								this.storage.storeTweetScreenshot(it.toString(), bytes.toByteArray(), readInverseMimeType()[".${this.format}"] ?:
									error("Invalid extension .${this.format}"))	
					}

		}
		obfuscator.close()

		val normal = TwitterScreenshotCrawler(this.storage)
		tweetIds.filter{this.storage.findTweetScreenshot(it) != null && this.storage.findScreenshot(it) == null}.forEach{

			val tweet = this.storage.findTweet(it)
					if(tweet != null){
//						val user = this.storage.findUser(tweet.userId)
//								val url = "https://twitter.com/${user!!.userId}/status/$it"
						val url = "https://twitter.com/any/status/$it" 
								val screenshot = normal.takeScreenshot(url)
								val bytes = ByteArrayOutputStream()
								ImageIO.write(screenshot.ensureOpaque(), this.format, bytes)
								this.storage.storeScreenshot(it.toString(), bytes.toByteArray(), readInverseMimeType()[".${this.format}"] ?:
									error("Invalid extension .${this.format}"))
					
					}

		}
		normal.close()
		
		//both screenshots are missing, now we use our code
		tweetIds.filter{this.storage.findScreenshot(it) == null}.forEach{

			val tweet = this.storage.findTweet(it)
					if(tweet != null){
//						val user = this.storage.findUser(tweet.userId)
//								val url = "https://twitter.com/${user!!.userId}/status/$it"
						val url = "https://twitter.com/any/status/$it" 
								val screenshot = this.takeScreenshots(url)
								val bytes = ByteArrayOutputStream()
								ImageIO.write(screenshot.first.ensureOpaque(), this.format, bytes)
								this.storage.storeScreenshot(it.toString(), bytes.toByteArray(), readInverseMimeType()[".${this.format}"] ?:
									error("Invalid extension .${this.format}"))
					val bytes_obs = ByteArrayOutputStream()
							ImageIO.write(screenshot.second.ensureOpaque(), this.format, bytes_obs)
								this.storage.storeTweetScreenshot(it.toString(), bytes_obs.toByteArray(), readInverseMimeType()[".${this.format}"] ?:
									error("Invalid extension .${this.format}"))	
					}

		}
	}
}

fun stitchScreenshots(imgs: List<BufferedImage>, pos: List<Int>): BufferedImage {
	if (imgs.size == 1)
		return imgs[0]

				val login_banner = 91
				val twitter_banner = 82
				val left_column = 100

				val height = pos.last() + imgs.last().height - login_banner // we substract what  we took out as the login banner

				imgs.map{it.height}.sum()

				val width = imgs.first().width
				var concatImage = BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)

				val g2d = concatImage.createGraphics()
				imgs.reversed().zip(pos.reversed()).forEach {
					//		println("$height ${it.second} ${it.first.getHeight()}")

					g2d.drawImage(it.first.getSubimage(0, 0, it.first.width, it.first.height - login_banner), 0, it.second, null)
				}

				//CHOP SEARCH VAR
				//	concatImage = concatImage.getSubimage(0, 107, concatImage.width, concatImage.height - 107)

				concatImage = concatImage.getSubimage(left_column, pos.first() + twitter_banner, concatImage.width - left_column, concatImage.height - pos.first() - twitter_banner)

						//	ImageIO.write(concatImage , "png", File("stiched.png") );

						return concatImage
}

