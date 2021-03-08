package edu.isistan.fakenews.webcrawler

import edu.isistan.fakenews.storage.MongoDBStorage
import org.jsoup.Connection
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.openqa.selenium.JavascriptExecutor
import org.openqa.selenium.OutputType
import org.openqa.selenium.TakesScreenshot
import org.openqa.selenium.WebDriver
import org.openqa.selenium.firefox.FirefoxBinary
import org.openqa.selenium.firefox.FirefoxDriver
import org.openqa.selenium.firefox.FirefoxOptions
import org.openqa.selenium.firefox.FirefoxProfile
import org.slf4j.LoggerFactory
import java.awt.image.BufferedImage
import java.io.*
import java.net.URI
import java.net.UnknownHostException
import java.nio.file.Files
import java.security.SecureRandom
import java.security.cert.CertificateException
import java.security.cert.X509Certificate
import java.util.regex.Pattern
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream
import javax.imageio.ImageIO
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLSocketFactory
import javax.net.ssl.TrustManager
import javax.net.ssl.X509TrustManager
import kotlin.math.pow
import kotlin.system.exitProcess
import java.nio.file.Paths


private var mimeType: Map<String, String>? = null
private var inverseMimeType: Map<String, String>? = null
fun readMimeTypes(): Map<String, String> {
    if (mimeType != null) {
        return mimeType!!
    }
    val result = mutableMapOf<String, String>()
    @Suppress("RECEIVER_NULLABILITY_MISMATCH_BASED_ON_JAVA_ANNOTATIONS")
//    val resourceFile = File(ClassLoader.getSystemClassLoader().getResource("mimeTypes.csv").file)	
//	val resourceFile = File(Paths.get(ClassLoader.getSystemClassLoader().getResource("mimeTypes.csv").toURI()).toString())
	val resourceFile = File("mimeTypes.csv")
	
    for (line in Files.readAllLines(resourceFile.toPath())) {
        val sep = line.split(",")
        if (sep.size != 2) {
            continue
        }
        result[sep[0]] = sep[1]
    }
    mimeType = result
    return result
}

fun readInverseMimeType(): Map<String, String>{
    if (inverseMimeType == null) {
        inverseMimeType = readMimeTypes().map { it.value to it.key }.toMap()
    }
    return inverseMimeType!!
}

var LOGGER = LoggerFactory.getLogger(WebHTMLCrawler::class.java)!!

data class WebErrors(var url: String, var error: String)

data class WebDocument(var mimeType: String = "",
                       var name: String = "",
                       var url: String = "",
                       var redirect: String? = null,
                       var data: ByteArray = ByteArray(0),
                       var errors: List<WebErrors> = emptyList()) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as WebDocument

        if (mimeType != other.mimeType) return false
        if (name != other.name) return false
        if (url != other.url) return false
        if (redirect != other.redirect) return false
        if (!data.contentEquals(other.data)) return false
        if (errors != other.errors) return false

        return true
    }

    override fun hashCode(): Int {
        var result = mimeType.hashCode()
        result = 31 * result + name.hashCode()
        result = 31 * result + url.hashCode()
        result = 31 * result + (redirect?.hashCode() ?: 0)
        result = 31 * result + data.contentHashCode()
        result = 31 * result + errors.hashCode()
        return result
    }
}

abstract class ScreenshotCrawler(val storage: MongoDBStorage, var format: String = "png"): Closeable {
    var driver: WebDriver

    init {
        LOGGER.info("Initializing Driver")
        val bin = System.getProperty("screenshot.bin")
        val driverBin = System.getProperty("screenshot.driver")
        val extensions = System.getProperty("screenshot.extensions")?.split(",")
        when {
            bin == null -> {
                LOGGER.error("Property screenshot.bin is not defined")
                exitProcess(0)
            }
            driverBin == null -> {
                LOGGER.error("Property screenshot.driver is not defined")
                exitProcess(0)
            }
        }
        System.setProperty("webdriver.gecko.driver", driverBin)
        val ffBinary = FirefoxBinary(File(bin))
        val fo = FirefoxOptions()
        fo.profile = FirefoxProfile()
        // if there are extensions, it adds them
        extensions?.forEach {
            LOGGER.info("Addind {} extension", it)
            fo.profile.addExtension(File(it))
        }
        fo.binary = ffBinary
        this.driver = FirefoxDriver(fo)
    }

    fun run() {
        LOGGER.info("Taking screenshots")
        this.storage.screenshotsToTake().forEach {
            LOGGER.info("Taking screenshot of {}", it)
            val screenshot = this.takeScreenshot(it)
            val bytes = ByteArrayOutputStream()
            ImageIO.write(screenshot.ensureOpaque(), this.format, bytes)
            this.storage.storeScreenshot(it, bytes.toByteArray(), readInverseMimeType()[".${this.format}"] ?:
                                                                    error("Invalid extension .${this.format}"))
        }
    }

		fun run(tweetIds : List<Long>) {
		init()
        LOGGER.info("Taking tweet screenshots")
		tweetIds.filter{this.storage.findScreenshot(it) == null}.forEach{
			
			val tweet = this.storage.findTweet(it)
			if(tweet != null){
				val user = this.storage.findUser(tweet.userId!!)
			val url = "https://twitter.com/${user!!.userId}/status/$it" 
			val screenshot = this.takeScreenshot(url)
            val bytes = ByteArrayOutputStream()
            ImageIO.write(screenshot.ensureOpaque(), this.format, bytes)
            
			this.storage.storeScreenshot(it.toString(), bytes.toByteArray(), readInverseMimeType()[".${this.format}"] ?:
                                                                    error("Invalid extension .${this.format}"))
			}
			
			//this is to obfuscate the image using ocr
//			val obfuscatedImage = obfuscateImage(processImage(screenshot,tweet),tweet,user.name)
//			val bytesObs = ByteArrayOutputStream()
//            ImageIO.write(obfuscatedImage.ensureOpaque(), this.format, bytesObs)
            
//			this.storage.storeTweetScreenshot(it.toString(), bytesObs.toByteArray(), readInverseMimeType()[".${this.format}"] ?:
//                                                             error("Invalid extension .${this.format}"))
			
		}
//		tweetIds.filter{this.storage.findTweetScreenshot(it) == null}.forEach{
//			
//			val tweet = this.storage.findTweet(it)!!
//			val user = this.storage.findUser(tweet.userId!!)!!
//			 
//            val file = storage.screenshotFS.openDownloadStream(tweet.tweetId.toString())
//			    val byte_array = file.readBytes()
//			
//         	val input_stream= ByteArrayInputStream(byte_array);
//		    val final_buffered_image = ImageIO.read(input_stream);
//			
//			val obfuscatedImage = obfuscateImage(processImage(final_buffered_image,tweet),tweet,user.name)
//			val bytesObs = ByteArrayOutputStream()
//            ImageIO.write(obfuscatedImage.ensureOpaque(), this.format, bytesObs)
//			
//			this.storage.storeTweetScreenshot(it.toString(), bytesObs.toByteArray(), readInverseMimeType()[".${this.format}"] ?:
//                                                             error("Invalid extension .${this.format}"))
//			
//		}
	}
	
    override fun close() {
        LOGGER.info("Closing driver")
//        this.driver.close()
		this.driver.quit() //apparently this is the one that closes everything
    }

    abstract fun takeScreenshot(url: String): BufferedImage
}

class SimpleScreenshotCrawler(storage: MongoDBStorage, format: String = "png"): ScreenshotCrawler(storage, format) {

    override fun takeScreenshot(url: String): BufferedImage {
        this.driver.get(url)
        this.driver.manage().window().fullscreen()
        val javascript = driver as JavascriptExecutor
        val screenshot = driver as TakesScreenshot
        val pageHeight = javascript.executeScript("return document.body.scrollHeight") as Long
        val windowHeight = driver.manage().window().size.height / 2
        var page = 0
        val pages = mutableListOf<BufferedImage>()
        //Nice Init
        while (page * windowHeight < pageHeight) {
            javascript.executeScript("window.scrollTo(0, ${page * windowHeight})")
            page++
        }
        page = 0
        //GET Screenshots
        while (page * windowHeight < pageHeight) {
            javascript.executeScript("window.scrollTo(0, ${page * windowHeight})")
            val img = screenshot.getScreenshotAs(OutputType.BYTES)
            pages.add(ImageIO.read(ByteArrayInputStream(img)))
            page++
        }
        if (pages.isEmpty()) {
            val img = screenshot.getScreenshotAs(OutputType.BYTES)
            return ImageIO.read(ByteArrayInputStream(img))
        }

        //CONCAT
        pages.dropLast(1).forEachIndexed { idx, img ->
            val w = img.width
            val h = img.height - 100
            val bi2 = BufferedImage(w, h, BufferedImage.TYPE_INT_RGB)
            val pixels = IntArray(w * h)
            img.getRGB(0, 0, w, h, pixels, 0 , w)
            bi2.setRGB(0, 0, w, h, pixels, 0 , w)
            pages[idx] = bi2
        }
        val height = pages.size * (windowHeight-50) + pages.last().height-50//pages.map { it.height }.sum()
        val width = pages.map { it.width }.max()?: pages[0].width
        val concatImage = BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
        val g2d = concatImage.createGraphics()
        var accHeight = height - windowHeight
        println("Window $windowHeight")
        pages.reversed().forEach {
            accHeight -= windowHeight
            println("acc $accHeight")
            g2d.drawImage(it, 0, accHeight, null)
        }
        g2d.dispose()

        return concatImage
    }
}

class OpenCVScreenshotCrawler(storage: MongoDBStorage, format: String = "png"): ScreenshotCrawler(storage, format) {
    init {
        LOGGER.info("Initializing OpenCV")
        initializeOpenCV()
    }
    override fun takeScreenshot(url: String): BufferedImage {
        this.driver.get(url)
        this.driver.manage().window().fullscreen()
        val javascript = driver as JavascriptExecutor
        val screenshot = driver as TakesScreenshot
        val pageHeight = javascript.executeScript("return document.body.scrollHeight") as Long
        val windowHeight = driver.manage().window().size.height / 2
        var page = 0
        val pages = mutableListOf<BufferedImage>()
        //Nice Init
        while (page * windowHeight < pageHeight) {
            javascript.executeScript("window.scrollTo(0, ${page * windowHeight})")
            page++
        }
        page = 0
        //GET Screenshots
        while (page * windowHeight < pageHeight) {
            javascript.executeScript("window.scrollTo(0, ${page * windowHeight})")
            val img = screenshot.getScreenshotAs(OutputType.BYTES)
            pages.add(ImageIO.read(ByteArrayInputStream(img)))
            page++
        }
        if (pages.isEmpty()) {
            val img = screenshot.getScreenshotAs(OutputType.BYTES)
            pages.add(ImageIO.read(ByteArrayInputStream(img)))
        }
        //CONCAT
        return pages.reduce {
                a, b -> verticalStich(a, b)
        }
    }
}

class WebHTMLCrawler(private val storage: MongoDBStorage)  {
    private var insecure = System.getProperty("web.ssl_insecure", "true")!!.toBoolean()
    private var max_retry = System.getProperty("web.max_retry").toIntOrNull() ?: 10
    private var limitExponentialBackoff = System.getProperty("web.max_wait").toLongOrNull() ?: 300_000

    fun run() {
        LOGGER.info("Downloading Web")
        val downloader = URLDownloader(insecure, max_retry, limitExponentialBackoff)
        while (true) {
            val url = this.storage.nextURLDownload()
            if (url == null) {
                LOGGER.info("No more data To Download")
                break
            }
            if (!this.storage.updateWebContent(url.url, url.tweetId)) {
                val webDocument = downloader.download(url.url)
                this.storage.storeWebContent(url.url,
                    url.tweetId,
                    webDocument.redirect,
                    webDocument.data,
                    webDocument.mimeType,
                    webDocument.errors.groupBy({ it.url }, { it.error }).mapValues { it.value[0] })
            }
            this.storage.removeURLDownload(url)
        }
    }

}


internal fun ZipOutputStream.addEntry(name: String, data: ByteArray) {
    this.putNextEntry(ZipEntry(name))
    this.write(data, 0, data.size)
    this.closeEntry()
}


internal class URLDownloader(private val insecure: Boolean = false, val max_retry: Int = 10, val limitExponentialBackoff: Long = 60_000){
    private val xmlContentTypeRxp = Pattern.compile("(application|text)/\\w*\\+?xml.*")
    private val textContent = "text/"
    val mimeType: Map<String, String> = readMimeTypes()
    private val sslSocketFactory: SSLSocketFactory = initSSLSocketFactory()

    fun download(url: String, baseURL: String?=null, max_retry: Int = this.max_retry, recursive: Boolean = true): WebDocument {
        LOGGER.info("Crawling {} with base URL {}", url, baseURL)
        var count = 0
        var retry = true
        val newUrl = when (baseURL) {
            null -> url
            else -> {
                try{
                    val uri = URI(baseURL)
                    uri.resolve(url).toString()
                } catch (e: Exception) {
                    return WebDocument(errors = listOf(WebErrors(url, e.toString())))
                }
            }
        }
        val result = WebDocument()
        result.url = newUrl
        while (retry) {
            try {
                val response = Jsoup.connect(newUrl).
                    sslSocketFactory(this.sslSocketFactory).
                    ignoreContentType(true).
                    execute()
                if (response.url().toString() != newUrl) {
                    result.redirect = response.url().toString()
                }
                if (recursive && isWebType(response.contentType())) {
                    processHtml(response, result)
                } else {
                    result.mimeType = response.contentType()
                    result.data = response.bodyAsBytes()
                    result.name = "file" + this.extension(result.mimeType)
                }
                retry = false
            } catch (e: IOException) {
                count++
                LOGGER.warn("Retry ", e)
                if (count > max_retry || e is UnknownHostException) {
                    result.errors = listOf(WebErrors(newUrl, e.message?: e.toString()))
                    retry = false
                } else {
                    val backOff =
                        (2.0.pow(count.toDouble()).toLong() * 1000).takeIf { it < this.limitExponentialBackoff }
                            ?: (this.limitExponentialBackoff + 1)
                    Thread.sleep(backOff)
                }
            }
        }
        return result
    }

    private fun processHtml(response: Connection.Response, result: WebDocument) {
        val byteOutputStream = ByteArrayOutputStream()
        val zip = ZipOutputStream(byteOutputStream)
        val document = response.parse()
        zip.addEntry("index_base.html", document.html().toByteArray())
        val errors = mutableListOf<WebErrors>()
        //Download images
        errors.addAll(processImgs(document, response, zip))
        // Download css
        errors.addAll(processCSS(document, response, zip))
        // Download JavaScript
        errors.addAll(processJavaScript(document, response, zip))

        zip.addEntry("index.html", document.html().toByteArray())
        zip.close()
        result.errors = errors
        result.name = "web.zip"
        result.mimeType = "text/html"
        result.data = byteOutputStream.toByteArray()
    }

    private fun processJavaScript(
        document: Document,
        response: Connection.Response,
        zip: ZipOutputStream
    ): List<WebErrors> {
        return document.getElementsByTag("script").filter {
            it.hasAttr("src")
        }.withIndex().map {
            val img = download(it.value.attr("src"), response.url().toString(),
                max_retry=2, recursive=false)
            if (img.errors.isEmpty()) {
                val name = it.index.toString() + ".js"
                zip.addEntry(name, img.data)
                it.value.attr("src", name)
            }
            img.errors
        }.flatten()
    }

    private fun processCSS(
        document: Document,
        response: Connection.Response,
        zip: ZipOutputStream
    ): List<WebErrors> {
        return document.getElementsByTag("link").filter {
            it.attr("rel") == "stylesheet"
        }.withIndex().map {
            val img = download(it.value.attr("href"), response.url().toString(),
                max_retry=2, recursive=false)
            if (img.errors.isEmpty()) {
                val name = it.index.toString() + ".css"
                zip.addEntry(name, img.data)
                it.value.attr("href", name)
            }
            img.errors
        }.flatten()
    }

    private fun processImgs(
        document: Document,
        response: Connection.Response,
        zip: ZipOutputStream
    ): List<WebErrors> {
        return document.getElementsByTag("img").filter {
            it.hasAttr("src") && !it.attr("src").startsWith("data:")
        }.withIndex().map {
            val img = download(it.value.attr("src"), response.url().toString(),
                max_retry=2, recursive=false)
            if (img.errors.isEmpty()) {
                val name = it.index.toString() + this.extension(img.mimeType)
                zip.addEntry(name, img.data)
                it.value.attr("src", name)
            }
            img.errors
        }.flatten()
    }


    private fun initSSLSocketFactory(): SSLSocketFactory {
        if (!this.insecure) {
            LOGGER.info("Creating standard SSL Socket Factory")
            return SSLContext.getDefault().socketFactory
        }

        LOGGER.info("Creating Insecure SSL Socket Factory")
        val trustAllCerts: Array<TrustManager> = arrayOf<TrustManager>(
            object : X509TrustManager {

                @Throws(CertificateException::class)
                override fun checkClientTrusted(arg0: Array<X509Certificate?>?, arg1: String?) {
                }

                @Throws(CertificateException::class)
                override fun checkServerTrusted(arg0: Array<X509Certificate?>?, arg1: String?) {
                }

                override fun getAcceptedIssuers(): Array<X509Certificate>? {
                    return null
                }
            }
        )

        try {
            val sc: SSLContext = SSLContext.getInstance("TLS")
            sc.init(null, trustAllCerts, SecureRandom())
            return sc.getSocketFactory()
        } catch (e: Exception) {
            LOGGER.warn("Error creating ssl connection")
        }
        LOGGER.warn("Fallback to standard SSL Socket Factory")
        return SSLContext.getDefault().socketFactory
    }

    private fun extension(mimeType: String): String {
        return this.mimeType.getOrDefault(mimeType, "")
    }

    private fun isWebType(type: String) = type.startsWith(textContent) || xmlContentTypeRxp.matcher(type).matches()

}