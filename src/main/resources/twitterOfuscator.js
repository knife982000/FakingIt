//profile pictures to black
var profPict = document.getElementsByClassName("avatar js-action-profile-avatar")
for (i=0; i< profPict.length; i++) {
    profPict[i].src = "data:image/gif;base64,R0lGODlhAQABAIAAAAUEBAAAACwAAAAAAQABAAACAkQBADs="
}
//mini profile pictures to black
var miniPict = document.getElementsByClassName("avatar size24 js-user-profile-link")
for (i=0; i< miniPict.length; i++) {
    miniPict[i].src = "data:image/gif;base64,R0lGODlhAQABAIAAAAUEBAAAACwAAAAAAQABAAACAkQBADs="
}

mapHandlers = {}
mapNames = {}
handlerCount = 1
nameCount = 1


//Handlers
var handlers = document.getElementsByClassName("username u-dir u-textTruncate")
for (i=0; i < handlers.length; i++) {
    var handler = handlers[i].children[0].lastChild.nodeValue
    if (!(handler in mapHandlers)) {
        mapHandlers[handler] = "handler_"+(handlerCount++)
    }
    handlers[i].children[0].lastChild.nodeValue=mapHandlers[handler]

}

//Names
var names = document.getElementsByClassName("fullname show-popup-with-id u-textTruncate")
for (i=0; i < names.length; i++) {
    var name = names[i].lastChild.nodeValue
    if (!(name in mapNames)) {
        mapNames[name] = "User_"+(nameCount++)
    }
    names[i].lastChild.nodeValue=mapNames[name]

}
//Quote Names
var names = document.getElementsByClassName("QuoteTweet-fullname u-linkComplex-target")
for (i=0; i < names.length; i++) {
    var name = names[i].lastChild.nodeValue
    if (!(name in mapNames)) {
        mapNames[name] = "User_"+(nameCount++)
    }
    names[i].lastChild.nodeValue=mapNames[name]
}

//Handlers in tweets
var handlers = document.getElementsByClassName("twitter-atreply pretty-link js-nav")
for (i=0; i < handlers.length; i++) {
    var handler = handlers[i].children[0].lastChild.nodeValue
    if (!(handler in mapHandlers)) {
        mapHandlers[handler] = "handler_"+(handlerCount++)
    }
    handlers[i].children[1].lastChild.nodeValue=mapHandlers[handler]

}