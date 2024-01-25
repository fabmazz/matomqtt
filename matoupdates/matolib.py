import requests
import json
import time


#url = "https://mapi.5t.torino.it/routing/v1/routers/mat/index/graphql/batch"
URL="https://mapi.5t.torino.it/routing/v1/routers/mat/index/graphql"
PARAMS = {
	"Content-Type": "application/json; charset=utf-8",
	#"Referer": "https://www.muoversiatorino.it/",
	#"Origin": "https://www.muoversiatorino.it",
	#"User-Agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67",
	"DNT" : "1",
	"Host": "mapi.5t.torino.it",

}
def make_request(operationName, variables, query):
	qar={"operationName":operationName,
	"variables":variables,"query":query}

	data_req = json.dumps(qar)#query_stop_arrivals#
	r = requests.post(URL,headers=PARAMS,data=data_req)

	return r

query_trip2="""
    query TripInfo($field: String!){
    trip(id: $field){
        gtfsId
        serviceId
        route{
            gtfsId
        }
        pattern{
            code
            
        }

    }
}
"""

def get_trip_info(gtfs_tripid): 
    #"gtt:23673879U"
    r = make_request("TripInfo",dict(field=gtfs_tripid), query=query_trip2)
    data = r.json()
    trip = data["data"]["trip"]
    return trip

query_pat="""
    query PatternInfo($field: String!){
    pattern(id: $field){
      name
      code
      semanticHash
      directionId
      headsign
      stops{
        gtfsId
        lat
        lon
      }
      patternGeometry{
        length
        points
      }
    }
}
"""
def get_pattern_info(patternCode): 
    #"gtt:23673879U"
    r = make_request("PatternInfo",dict(field=patternCode), query=query_pat)
    data = r.json()
    #
    return data["data"]["pattern"]
