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
		Thread.sleep(5000)//Wait to load... it is horrible but it kind of work!
		while (lastTop != windowScrollY(javascript).toInt()) {
			javascript.executeScript("""
				if (document.getElementsByClassName("css-18t94o4 css-1dbjc4n r-1777fci r-1jayybb r-o7ynqc r-1j63xyz r-13qz1uu").length == 1) {
			    	document.getElementsByClassName("css-18t94o4 css-1dbjc4n r-1777fci r-1jayybb r-o7ynqc r-1j63xyz r-13qz1uu")[0].click()
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

class TwitterObfuscatedScreenshotCrawler(storage: MongoDBStorage, format: String = "jpg"): ScreenshotCrawler(storage, format) {

	val obfuscatorJS = run {
		val resourceFile = File(ClassLoader.getSystemClassLoader().getResource("twitterObfuscator.js").file)
				Files.readAllLines(resourceFile.toPath()).reduce {
			a, b -> "$a\n$b"
		}
	}

	val varsObfuscatorJS = run {
		val resourceFile = File(ClassLoader.getSystemClassLoader().getResource("twitterObfscatorVariables.js").file)
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
			Thread.sleep(1000)//Wait to load... it is horrible but it kind of work!
			data = javascript.executeScript("data = ${gson.toJson(data)}\n"+this.obfuscatorJS)
			val pos = windowScrollY(javascript).toInt()
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
					val user = this.storage.findUser(tweet!!.userId)
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
	val height = pos.last() + imgs.last().height
	val width = imgs.first().width
	var concatImage = BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
	val g2d = concatImage.createGraphics()
	imgs.reversed().zip(pos.reversed()).forEach {
		g2d.drawImage(it.first, 0, it.second, null)
	}
	//CHOP SEARCH VAR
	concatImage = concatImage.getSubimage(0, 107, concatImage.width, concatImage.height - 107)
	return concatImage
}

