package edu.isistan.fakenews.scrapper

import com.google.gson.Gson
import java.net.ServerSocket
import org.openqa.selenium.WebDriver
import org.openqa.selenium.firefox.FirefoxBinary
import org.openqa.selenium.firefox.FirefoxDriver
import org.openqa.selenium.firefox.FirefoxOptions
import org.openqa.selenium.firefox.FirefoxProfile
import org.openqa.selenium.support.ui.WebDriverWait
import java.io.File
import java.io.InputStream
import java.lang.StringBuilder
import java.net.Socket
import java.net.SocketException
import java.util.concurrent.Semaphore
import kotlin.system.exitProcess


fun initFirefoxWithScrapperExtension(): WebDriver {
    val bin = System.getProperty("screenshot.bin")
    val driverBin = System.getProperty("screenshot.driver")
    val debuggerPort = getFreePort()
    when {
        bin == null -> {
            edu.isistan.fakenews.webcrawler.LOGGER.error("Property screenshot.bin is not defined")
            exitProcess(0)
        }
        driverBin == null -> {
            edu.isistan.fakenews.webcrawler.LOGGER.error("Property screenshot.driver is not defined")
            exitProcess(0)
        }
    }
    System.setProperty("webdriver.gecko.driver", driverBin)
    val ffBinary = FirefoxBinary(File(bin))

    val fo = FirefoxOptions()
    //fo.setHeadless(true)
    fo.addArguments("-start-debugger-server", debuggerPort.toString())
    fo.profile = FirefoxProfile()
    fo.profile.setPreference("devtools.debugger.remote-enabled", true)
    fo.profile.setPreference("devtools.debugger.prompt-connection", false)
    fo.profile.setPreference("devtools.chrome.enabled", true)
    fo.binary = ffBinary
    val driver = FirefoxDriver(fo)

    WebDriverWait(driver, 10)

    val extensionFile = File(System.getProperty("tweeterScrapper.extensions", "no-file"))
    if (!extensionFile.exists()) {
        LOGGER.error("Extension directory ${extensionFile.absolutePath} not found. Check tweeterScrapper.extension configuration")
        exitProcess(1)
    }
    loadExtension(extensionFile.absolutePath, debuggerPort)
    return driver
}

private fun getFreePort(): Int {
    val server = ServerSocket(0)
    val port = server.localPort
    server.close()
    return port
}

private class ReadMessages(val iis: InputStream, val callback: (String)->Unit): Runnable {
    override fun run() {
        val bytes = ByteArray(1024)
        var first = true
        val sb = StringBuilder()
        var size = 0
        var sep = 0
        while (true) {
            try {
                val len = iis.read(bytes)
                if (len == -1) {
                    break
                }
                val msg = String(bytes.sliceArray(0 until len))
                if (first) {
                    sep = msg.indexOf(':')
                    size = msg.substring(0 until sep).toInt()
                    if ((msg.length - sep - 1) == size) {
                        this.callback(msg)
                    } else {
                        first = false
                        sb.append(msg)
                    }
                } else {
                    sb.append(msg)
                    if ((sb.length - sep - 1) == size) {
                        this.callback(sb.toString())
                        sb.clear()
                    }
                }
            } catch (e: SocketException) {
                break
            }
        }
    }
}


private fun loadExtension(path: String, port: Int) {
    val socket = Socket("localhost",port)
    val ready = Semaphore(0)

    val iis = socket.getInputStream()
    val oos = socket.getOutputStream()

    var msg = 0
    var target = ""
    Thread(ReadMessages(iis) {
        msg++
        //println(it)
        when (msg) {
            1 -> {
                oos.write("31:{\"to\":\"root\",\"type\":\"listTabs\"}".toByteArray())
                oos.flush()
            }
            2 -> {
                val gson = Gson()
                val sep = it.indexOf(":")
                target = gson.fromJson(it.substring(sep + 1), Map::class.java)["addonsActor"] as String
                LOGGER.debug("Firefox addon actor: {}", target)
                //println("Actor is: $target")
                val payloadMap =
                    mapOf<String, String>("to" to target, "type" to "installTemporaryAddon", "addonPath" to path)
                var payload = gson.toJson(payloadMap)
                oos.write("${payload.length}:$payload".toByteArray())
                oos.flush()
                Thread.sleep(1000)
                socket.close()
                ready.release()
            }
        }
    }).start()

    ready.acquire()
}
