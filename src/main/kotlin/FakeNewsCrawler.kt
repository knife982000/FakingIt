package edu.isistan.fakenews

import edu.isistan.fakenews.crawler.DEBUG_CRAWLER
import edu.isistan.fakenews.crawler.TwitterCrawler
import edu.isistan.fakenews.storage.DEBUG_DB
import edu.isistan.fakenews.storage.MongoDBStorage
import org.apache.commons.cli.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.File
import kotlin.system.exitProcess


private class FakeNewsCrawler

val LOGGER = LoggerFactory.getLogger(FakeNewsCrawler::class.java)


fun showHelp(options: Options) {
    val helpFormatter = HelpFormatter()
    helpFormatter.printHelp("edu.isistan.fakenews.FakeNewsCrawlerKt",
        "Tool for querying and download tweets and their related URLs", options,"", true)
    exitProcess(0)
}

fun main(args: Array<String>) {
    val options = Options()
    val groups = OptionGroup()
    groups.isRequired = true
    groups.addOption(Option("d", "download", false, "start the download process"))
    val add = Option("a", "add", true, "add queries for download. File is a text file with one query per line")
    add.argName = "file"
    add.args = 1
    groups.addOption(add)
    groups.addOption(Option("h", "help", false, "display this help and exit"))
    options.addOptionGroup(groups)

    options.addOption(Option("ddb", "fakeDB", false, "set the FakeNewsTest databese instead of the FakeNews (for testing purpose)"))

    val parser: CommandLineParser = DefaultParser()
    try { // parse the command line arguments
        val line = parser.parse(options, args)
        if (options.hasOption("ddb"))
            DEBUG_DB = true
        when {
            line.hasOption("h") -> showHelp(options)
            line.hasOption("d") -> download()
            line.hasOption("a") -> addQueries(line.getOptionValue("a"), options)
        }
    } catch (exp: ParseException) { // problem with the parameters, help is needed
        println("Parsing failed.  Reason: ${exp.message}")
        showHelp(options)
    }

}

fun addQueries(filename: String?, options: Options) {
    if (filename == null){
        println("File name is undefined")
        showHelp(options)
    }
    val file = File(filename)
    if (!file.exists() || !file.isFile) {
        println("The specified root is not a file or does not exists")
        showHelp(options)
    }
    LOGGER.info("Initializing DB")
    val storage = MongoDBStorage()
    storage.init()
    file.readLines().map { it.trim() }.
        filter { it.isNotEmpty() }.
        forEach {
            LOGGER.debug("Adding query: \"{}\"", it)
            storage.findOrStoreQuery(it)
        }
    storage.close()
}

fun download() {
    LOGGER.info("Initializing DB")
    val storage = MongoDBStorage()
    storage.init()
    var twitterCrawler = TwitterCrawler(storage)
    twitterCrawler.init()
    twitterCrawler.run()
    storage.close()
}
