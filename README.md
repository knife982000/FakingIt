# Faking It! A tool for collecting multi-sourced datasets powered by Social Media

This Java tool aims at facilitating the creation of datasets from social media including not only the shared content, but also the social context of the content. 

## Overview

Social media has become the primary source of news for their users. Besides fostering social connections between persons, social networks also represent the ideal environment for undesirable phenomena, such as the dissemination of unwanted or aggressive content, misinformation and fake news, which all affect the individuals as well as the society as a whole. Thereby, in the last few years, the research on misinformation has received increasing attention. Nonetheless, even though some computational solutions have been presented, the lack of a common ground and public datasets has become one of the major barriers. Not only datasets are rare, but also, they are mostly limited to only the actual shared text, neglecting the importance of other features, such as social content and temporal information. In this scenario, this project proposes the creation of a publicly available dataset, comprising multi-sourced data and including diverse features related not only to the textual and multimedia content, but also to the social context of news and their temporal information. This dataset would not only allow tackling the task of fake news detection, but also studying their evolution, which, in turn, can foster the development of mitigation and debunking techniques. 

The Figure shows the diffent types of data that can be retrived using *Faking It!*. 

![]()

In summary, *Faking It!* allows to collect:

* Web content retrieved from urls given as input.
* Social posts referring to the selected news.
* Temporal diffusion of posts trajectory (for example, retweet or replies chains).
* URL and images accompanying the retrieved posts.
* For each user engaged in the diffusion process, the profile, posts and social network.

The original aim of the tool was collecting datasets related to the evolution and propagation of fake news. In this context, the collected multi-source datasets have the potential to contribute in the study of various open research problems related to fake news. The augmented set of features provides a unique opportunity to play with different techniques for detecting and analysing the evolution of fake news. Moreover, the temporal information enables the study of the processes of fake news diffusion and evolution, and the roles that users play in such processes through social media reactions. The proposed dataset could be the starting point of diverse exploratory studies and potential applications:
* *Fake news detection.* One of the main difficulties is the lack of reliable labelled data and an extended feature set. The presented dataset aims at providing reliable labels annotated by humans and multi-sourced features including text, images, social context and temporal information.
* *Fake news evolution and engagement cycle.* The diffusion process involves different stages in users’ engagement and reaction. Using the temporal information could help to study how stories become viral and their diffusion trajectories across social media. This would also allow to study the roles that users play on the diffusion.
* *Fake and malicious account detection.* These types of accounts include social bots, trolls and spammers, which can all boost the spread of fake news. The user features in the dataset would allow studying the characteristics of such type of users, how they differentiate from legitimate users and how they contribute to the spread of disinformation.
* *Fake news debunking.* Knowing the evolution and trajectories of fake news could help in the study of the debunking process. Likewise, user reactions and answers in social media can provide useful elements to help to determine the veracity of a piece of news.


## Installation

Source code is hosted on Github. You can download this project in either zip or tar formats.

Additionally, you can clone the project with Git by running:
```
$ git clone https://github.com/knife982000/FakingIt.git
```

Faking It! can be built from the source code using Gradle, running
```
$ gradlew clean build
```

### Requirements

Currently, the tool only supports Twitter as social meddia. Hence for retrieving the data, the tool requires configuring the corresponding Twitter API keys. API keys can be obtained from the [Twitter Developers portal](https://developer.twitter.com/en/docs/basics/authentication/guides/access-tokens.html).

Data is storaged in a [Mongo](https://www.mongodb.com/) database, hence for running *Faking It!* it is neccesary to have a MongoDB instance running. Several collections are created to store all the retrieved content. Indexes are also created.

### Configuration

As the proposed dataset will include information from multiple sources, the tool provides options to retrieve specific subsets of data, such as news content, news images, related images, related news, social media context, related users’ metadata, social networks, amongst others, based on the specific input given to the tool. 

#### Options:

*  ``-a,--add <file>`` add queries for download. File is a text file with one query per line
*  ``-all,--all``  downloads user tweets or relations for all users in the database. By default, only for authors of core tweets (i.e. tweets directly found with the queries)
*  ``-conf,--configuration <arg>`` set the configuration property file, By default: settings.properties
*  ``-d,--download`` start the query download process
*  ``-h,--help`` display this help and exit
*  ``-rec,--recursive`` recursively download all replies chains (replies of replies). By default it does not download replies of replies
*  ``-search,--search <file>`` search tweets by matching content. For each searched tweet, we only keep the closest
*  ``-s,--screenshot`` start the screenshot download process for all web content in the database.
*  ``-t,--tweets <file>`` start the tweet download process
*  ``-tr,--tweets <file>`` start the in reply to download process (only the tweets)
*  ``-track,--track`` track topics, locations or users in real time. Configurations in property file
*  ``-trf,--tweets <file>`` start the full in reply to download process (the tweets + favorites and retweets)
*  ``-u,--users`` downloads the info of missing users 
 *  ``-ur,--user relations``  downloads the followee/follower relations of the already downloaded users. By default only downloads for users who wrote the core tweets. With -all downloads for every user in the database
 *  ``-ut,--user tweets`` downloads the tweets of the already downloaded users. By default only downloads for users who wrote the core tweets. With -all downloads for every user in the database
 *  ``-w,--web``start the Web content download process (i.e. for the urls found in queries or tweets)
 
#### Configuration File:

* ``twitter.oauthdir`` directory with the ``.oauth`` for accessing the Twitter API.
* ``ddb.name`` the name for the database. The default name is ``FakeNewsTest``.

Depending on the download mode, different configurations could be required:

* Content crawler properties
  * ``web.ssl_insecure=true``
  * ``web.max_retry=10``
  * ``web.max_wait=300_000``

* Screenshot crawler properties
  * ``screenshot.bin`` Location of runnable firefox
  * ``screenshot.driver``=geckodriver.exe

* Screenshot configurations
  * ``screenshot.height`` recommended 600
  * ``screenshot.width`` recommended 800
  * ``tweeterScrapper.extensions=TwitterScrapper-FirefoxExtension`` needed for replies download

* Twitter streaming api properties (for real time crawling)
  * ``stream.language`` language to filter the tweets
  * ``stream.topics`` comma separated list of topics to track, it can include hashtags
  * ``stream.locations`` bounding box of the location to search for tweets
  * ``stream.users`` usernames to track
  * ``stream.max_tweets`` number of tweets to retrieve

* Configuration to complement the search queries
  * ``search.since`` earliest date to retrieve tweets. Format ``YYYY-MM-DD``
  * ``search.until`` latest date to retrieve tweets. Format ``YYYY-MM-DD``
  * ``query.resultType`` whether to include retweets
  * ``search.location`` bounding box of the location to search for tweets
  * ``search.language`` language to filter the tweets

## Future releases

Future releases of the tool will include:
* Google search of the images found in the social posts.
* Query augmentation for finding related news.
* Visualization of the download content.
* Option for getting news from specific news or media outlets.

## Contact info:

* Juan Manuel Rodriguez ([juanmanuel.rodriguez@isistan.unicen.edu.ar](mailto:juanmanuel.rodriguez@isistan.unicen.edu.ar))
* Antonela Tommasel ([antonela.tommasel@isistan.unicen.edu.ar](mailto:antonela.tommasel@isistan.unicen.edu.ar))


*Faking it!* is licenced under the Apache License V2.0. Copyright 2020 - ISISTAN - UNICEN - CONICET
