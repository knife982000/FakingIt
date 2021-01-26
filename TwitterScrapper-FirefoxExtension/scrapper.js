//Debugging only
function logError(error) {
    console.log("Error: ")
    console.log(error)
}
//Debugging only
function logOk(ok) {
    console.log("Ok: ")
    console.log(ok)
}


function repliesInterceptor(details) {
    let filter = browser.webRequest.filterResponseData(details.requestId);
    let decoder = new TextDecoder("utf-8");
    let encoder = new TextEncoder();
  
    data = []
    //push all the data into an array.
    filter.ondata = event => {
        data.push(event.data)
        filter.write(event.data)
    }
    //push all the data into an array.
    filter.onstop = event => {
      let str = "";
      if (data.length == 1) {
          str = decoder.decode(data[0])
      } else {
        for(i=0; i< data.length; i++){ 
            let stream = !(i == data.length - 1)
            str += decoder.decode(data[i], {stream})
        }  
      }
      //Extracts tweet data
      try {
          let tweets = JSON.parse(str)['globalObjects']['tweets']
          //It sends the data to a simple http rest service.
          let xhttp = new XMLHttpRequest()
          xhttp.addEventListener("error", logError)
          xhttp.addEventListener("load", logOk)
          xhttp.open("POST", "http://localhost:8080/replies")
          xhttp.send(JSON.stringify(tweets))
      } catch (error) {
          let xhttp = new XMLHttpRequest()
          xhttp.addEventListener("error", logError)
          xhttp.addEventListener("load", logOk)
          xhttp.open("POST", "http://localhost:8080/error")
          xhttp.send(JSON.stringify({"error": error, "data": str}))
      }
      //filter.write(encoder.encode(str));
      filter.disconnect();
    }
  
    return {};
}

function searchInterceptor(details) {
    let filter = browser.webRequest.filterResponseData(details.requestId);
    let decoder = new TextDecoder("utf-8");
    let encoder = new TextEncoder();
  
    data = []
    //push all the data into an array.
    filter.ondata = event => {
        data.push(event.data)
        filter.write(event.data)
    }
    //reconstructs the data and process it.
    filter.onstop = event => {
      let str = "";
      if (data.length == 1) {
          str = decoder.decode(data[0])
      } else {
        for(i=0; i< data.length; i++){ 
            let stream = !(i == data.length - 1)
            str += decoder.decode(data[i], {stream})
        }  
      }
      console.log(str)
      //It sends the data to a simple http rest service.
      let tweets = JSON.parse(str)['globalObjects']['tweets']
      let users = JSON.parse(str)['globalObjects']['users']
      let xhttp = new XMLHttpRequest()
      xhttp.addEventListener("error", logError)
      xhttp.addEventListener("load", logOk)
      xhttp.open("POST", "http://localhost:8080/search")
      xhttp.send(JSON.stringify({"tweets": tweets, "users": users}))

      //filter.write(encoder.encode(str));
      filter.disconnect();
    }
  
    return {};
}

//Listen for twitter API in replies
browser.webRequest.onBeforeRequest.addListener(
    repliesInterceptor,
    {urls: ["https://twitter.com/i/api/2/timeline/conversation/*"]},
    ["blocking"]
);
  

//Listen for twitter API in searchs
browser.webRequest.onBeforeRequest.addListener(
    searchInterceptor,
    {urls: ["https://twitter.com/i/api/2/search/adaptive.json?*"]},
    ["blocking"]
);
  