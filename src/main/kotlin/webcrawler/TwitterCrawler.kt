package edu.isistan.fakenews.webcrawler

import edu.isistan.fakenews.webcrawler.ScreenshotCrawler
import edu.isistan.fakenews.storage.MongoDBStorage
import java.awt.image.BufferedImage
import org.openqa.selenium.JavascriptExecutor
import org.openqa.selenium.support.ui.WebDriverWait
import org.openqa.selenium.TakesScreenshot
import org.openqa.selenium.OutputType
import java.io.ByteArrayInputStream
import javax.imageio.ImageIO
import java.io.ByteArrayOutputStream
import java.io.File
import java.nio.file.Files

class TwitterScreenshotCrawler(storage: MongoDBStorage, format: String = "png"): ScreenshotCrawler(storage, format) {

	override fun takeScreenshot(url: String): BufferedImage {
		this.driver.get(url)
		this.driver.manage().window().fullscreen()
		WebDriverWait(this.driver, 10)
		val javascript = driver as JavascriptExecutor
		val screenshot = driver as TakesScreenshot
		var pageHeight = javascript.executeScript("return document.getElementById(\"permalink-overlay\").scrollHeight") as Long
		val windowHeight = driver.manage().window().size.height / 2

		var page = 0
		val pages = mutableListOf<BufferedImage>()
		var positions = mutableListOf<Long>()
		//Nice Init
		while (page * windowHeight < pageHeight) {
			javascript.executeScript("document.getElementById(\"permalink-overlay\").scrollTo(0, ${page * windowHeight})")
			Thread.sleep(2000)
			page++
			javascript.executeScript("""
					var button = document.getElementsByClassName("ThreadedConversation-showMoreThreadsButton u-textUserColor")
					if (button.length == 1) {
						button[0].click()
					}
					""".trimIndent())
					javascript.executeScript("""
							var shows = document.getElementsByClassName("Tombstone-action js-display-this-media btn-link")
							for (i=0; i<shows.length; i++) {
								shows[i].click()
							}
							""".trimIndent())
							pageHeight = javascript.executeScript("return document.getElementById(\"permalink-overlay\").scrollHeight") as Long
		}
		page = 0
				//GET Screenshots
				while (page * windowHeight < pageHeight) {
					javascript.executeScript("document.getElementById(\"permalink-overlay\").scrollTo(0, ${page * windowHeight})")
					val img = screenshot.getScreenshotAs(OutputType.BYTES)
					pages.add(ImageIO.read(ByteArrayInputStream(img)))
					positions.add(javascript.executeScript("return document.getElementById(\"permalink-overlay\").scrollTop") as Long)
					//println("Moved to ${page * windowHeight} recognized as ${positions.last()}")
					page++
				}
		positions.add(pageHeight)

		val pixelSize = pages[0].height.toDouble() / driver.manage().window().size.height
		return stitchScreenshots(pages, positions.map { (it * pixelSize).toInt() })
	}


}

class TwitterOfuscatedScreenshotCrawler(storage: MongoDBStorage, format: String = "jpg"): ScreenshotCrawler(storage, format) {

	val ofuscatorJS = run {
		val resourceFile = File(ClassLoader.getSystemClassLoader().getResource("twitterOfuscator.js").file)
				Files.readAllLines(resourceFile.toPath()).reduce {
			a, b -> "$a\n$b"
		}
	}
	override fun takeScreenshot(url: String): BufferedImage {
		this.driver.get(url)
		this.driver.manage().window().fullscreen()
		WebDriverWait(this.driver, 10)
		val javascript = driver as JavascriptExecutor
		val screenshot = driver as TakesScreenshot
		var pageHeight = javascript.executeScript("return document.getElementById(\"permalink-overlay\").scrollHeight") as Long
		//var windowHeightr = javascript.executeScript("return window.innerHeight") as Long
		val windowHeight = driver.manage().window().size.height / 2

		var page = 0
		val pages = mutableListOf<BufferedImage>()
		var positions = mutableListOf<Long>()
		//Nice Init
		while (page * windowHeight < pageHeight) {
			javascript.executeScript("document.getElementById(\"permalink-overlay\").scrollTo(0, ${page * windowHeight})")
			Thread.sleep(2000)
			page++
			javascript.executeScript("""
					var button = document.getElementsByClassName("ThreadedConversation-showMoreThreadsButton u-textUserColor")
					if (button.length == 1) {
						button[0].click()
					}
					""".trimIndent())
					javascript.executeScript("""
							var shows = document.getElementsByClassName("Tombstone-action js-display-this-media btn-link")
							for (i=0; i<shows.length; i++) {
								shows[i].click()
							}
							""".trimIndent())
							pageHeight = javascript.executeScript("return document.getElementById(\"permalink-overlay\").scrollHeight") as Long
		}
		javascript.executeScript(this.ofuscatorJS)
		pageHeight = javascript.executeScript("return document.getElementById(\"permalink-overlay\").scrollHeight") as Long
		page = 0
		//GET Screenshots
		while (page * windowHeight < pageHeight) {
			javascript.executeScript("document.getElementById(\"permalink-overlay\").scrollTo(0, ${page * windowHeight})")
			val img = screenshot.getScreenshotAs(OutputType.BYTES)
			pages.add(ImageIO.read(ByteArrayInputStream(img)))
			positions.add(javascript.executeScript("return document.getElementById(\"permalink-overlay\").scrollTop") as Long)
			//println("Moved to ${page * windowHeight} recognized as ${positions.last()}")
			page++
		}
		positions.add(pageHeight)

		val pixelSize = pages[0].height.toDouble() / driver.manage().window().size.height
		return stitchScreenshots(
				pages,
				positions.map { (it * pixelSize).toInt() })
	}

	fun run(tweetIds : ArrayList<Long>){
		init()
		LOGGER.info("Taking obfuscated tweet screenshots")
		
//		super.run(tweetIds) 
		
		tweetIds.filter{this.storage.findTweetScreenshot(it) == null}.forEach{

			val tweet = this.storage.findTweet(it)
					val user = this.storage.findUser(tweet!!.userId!!)
					val url = "https://twitter.com/${user!!.userId}/status/$it" 
					val screenshot = this.takeScreenshot(url)
					val bytes = ByteArrayOutputStream()
					ImageIO.write(screenshot.ensureOpaque(), this.format, bytes)

					val rectangleImage = getTweetRectangle(screenshot)
					val bytesObs = ByteArrayOutputStream()
					ImageIO.write(rectangleImage.ensureOpaque(), this.format, bytesObs)
			
					this.storage.storeTweetScreenshot(it.toString(), bytesObs.toByteArray(), readInverseMimeType()[".${this.format}"] ?:
						error("Invalid extension .${this.format}"))	
		}
	}
}

fun stitchScreenshots(imgs: List<BufferedImage>, pos: List<Int>): BufferedImage {
	if (imgs.size == 1)
		return imgs[0]
				val height = pos.last()
				val width = imgs[0].width
				val concatImage = BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
				val g2d = concatImage.createGraphics()
				imgs.zip(pos.dropLast(1)).forEach {
		g2d.drawImage(it.first, 0, it.second, null)
	}
	return concatImage
}


