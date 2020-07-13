# FakingIt

Social Media Fake News Crawler.

This Java tool aims at facilitating the creation of datasets from social media including not only the shared content, but also the social context of the content. 

## Overview

![]()

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

Currently, the tool only supports Twitter as social meddia. Hence for retrieving the data, the tool will require configuring the corresponding Twitter API keys. API keys can be obtained from the [Twitter Developers portal](https://developer.twitter.com/en/docs/basics/authentication/guides/access-tokens.html).

### Configuration

As the proposed dataset will include information from multiple sources, the tool will provide options to retrieve specific subsets of data, such as news content, news images, related images, related news, social media context, related users’ metadata, social networks, amongst others, based on the specific input given to the tool. 



## Contact info:

* Juan Manuel Rodriguez ([juanmanuel.rodriguez@isistan.unicen.edu.ar](mailto:juanmanuel.rodriguez@isistan.unicen.edu.ar))
* Antonela Tommasel ([antonela.tommasel@isistan.unicen.edu.ar](mailto:antonela.tommasel@isistan.unicen.edu.ar))


*Faking it!* is licenced under the Apache License V2.0. Copyright 2020 - ISISTAN - UNICEN - CONICET
