
import logging, requests, collections
from collections import OrderedDict
#import bson.json_util 
#json_util import dumps
#import simplejson as json

from spyne import Application, srpc, ServiceBase, Iterable, Unicode, UnsignedInteger, \
    String

from spyne.protocol.json import JsonDocument
from spyne.protocol.http import HttpRpc
from spyne.server.wsgi import WsgiApplication

class CrimeReport(ServiceBase):   
           
    @srpc(float,float,float,_returns=Iterable(Unicode))
    

    def checkcrime(lon,lat,radius):
        param ={'lon' : lon, 'lat' : lat, 'radius': radius, 'key': '.' }
        response = requests.get("https://api.spotcrime.com/crimes.json", params = param)
        data = response.json()
        givenList = data["crimes"]
        
        crimeList = []
        addressList = []
        OF = "OF"
        AMPERSAND = "&"
        #icrimecount = []
        for chunk in givenList:
            if chunk["type"] in crimeList:
                pass
            else:
                crimeList.append(chunk["type"])
            
            fullAddress = chunk["address"]
            
            #print fullAddress
            
            if (OF in fullAddress):
                street1 = fullAddress[fullAddress.index(OF) + 3 :]

                if street1 in addressList:
                    pass
                else:
                    addressList.append(street1)
            elif (AMPERSAND in fullAddress):
                street2 = fullAddress[:fullAddress.index(AMPERSAND)]
                street3 = fullAddress[fullAddress.index(AMPERSAND)+2:]

                if street2 in addressList:
                    pass
                else:
                    addressList.append(street2)
                
                if street3 in addressList:
                    pass
                else:
                    addressList.append(street3)


        icrimecount = [0] * len(crimeList)
        time_crime_slot = [0] * 8 #there are eight different time slots which are fixed 12 am - 3am , 3am - 6am etc etc.
        
        istreetcount = [0]* len(addressList)
        

        for chunk in givenList:
            if chunk["type"] in crimeList:
                icrimecount[crimeList.index(chunk["type"])] +=1

            eventTime = 0
            #12 is the only number in PM in which we don't add 12
            
            event_hour = int(chunk["date"][9:11])*100
            event_min = int(chunk["date"][12:14])

            #print chunk["date"][15:17]
            if ((str(chunk["date"][15:17]) == 'PM') and (int(event_hour) != 1200 )):
                eventTime +=1200
                                        
            if ((str(chunk["date"][15:17]) == 'AM') and (int(event_hour) == 1200 )):
                event_hour = 2400

            eventTime += event_hour + event_min

            value = 300
            #print eventTime
            for i in range(8):
                if (int(eventTime) <= value):
                    time_crime_slot[i] += 1
                    break
                value += 300
            
            for street in addressList:
                if street in chunk["address"]:
                    istreetcount[addressList.index(street)] += 1    

            
        #sorted(range(len(addressList)), key=lambda x: addressList[x])
        #topThree = sorted(range(len(addressList)), key=lambda x: addressList[x])[-3:]

        
        #print icrimecount       
        total_crime = len(givenList)
        #print time_crime_slot

        print addressList
        #print crimeList

        print istreetcount

        #print topThree

        crimeListDict = {}

        for i in range(len(crimeList)):
            crimeListDict[str(crimeList[i])] = icrimecount[i]

        dangerousList = {} #stores the key value pairs of top most dangerous streets with street names as keys and event count as values
        top = 0 #Stores the name of the topmost dangerous street
        second = 0 #Stores the name of the second-most dangerous street
        third = 0 #Stores the name of the third-most dangerous street
        secondIndex = 0 #Stores the index of the topmost dangerous street
        thirdIndex = 0 #Stores the index of the second most dangerous street
        topIndex = 0 #Stores the index of the third-most dangerous street
        for i in range(len(istreetcount)):
            if (istreetcount[i] >= top):
                third = second
                second = top
                top = istreetcount[i]
                thirdIndex = secondIndex
                secondIndex = topIndex
                topIndex = i
            elif (istreetcount[i] >= second):
                third = second
                second = istreetcount[i]
                thirdIndex = secondIndex
                secondIndex = i
            elif (istreetcount[i] >= third):
                third = istreetcount[i]
                thirdIndex = i

            
        #Mapping the key value pairs
        dangerousList[str(addressList[topIndex])] = istreetcount[topIndex]
        dangerousList[str(addressList[secondIndex])] = istreetcount[secondIndex]
        dangerousList[str(addressList[thirdIndex])] = istreetcount[thirdIndex]        

        print dangerousList
        #print givenList
        #for i in range(len(addressList)):
        #    dangerousList[str(addressList[i])] = istreetcount[i]

        #print dangerousList
        finalReport = OrderedDict() 
        #final["total_crime"]
        finalReport ={"total_crime": len(givenList),
          "the_most_dangerous_streets": dangerousList.keys(),
          "crime_count_type":crimeListDict,
          "event_time_count" : {
            "12:01am-3am" : time_crime_slot[0],
            "3:01am-6am" : time_crime_slot[1],
            "6:01am-9am" : time_crime_slot[2],
            "9:01am-12noon" : time_crime_slot[3],
            "12:01pm-3pm" : time_crime_slot[4],
            "3:01pm-6pm" : time_crime_slot[5],
            "6:01pm-9pm" : time_crime_slot[6],
            "9:01pm-12midnight" : time_crime_slot[7]
            }
        
        }

        print type(finalReport)
        finalReport=collections.OrderedDict(finalReport)
        yield finalReport
        
    
if __name__ == '__main__':
    # Python daemon boilerplate
    from wsgiref.simple_server import make_server

    logging.basicConfig(level=logging.DEBUG)
    application = Application([CrimeReport], 'spyne.crime.http',in_protocol=HttpRpc(validator='soft'),out_protocol=JsonDocument(ignore_wrappers=True))

    wsgi_application = WsgiApplication(application)

    server = make_server('0.0.0.0', 8000, wsgi_application)

    logging.info("listening to http://0.0.0.0:8000")
    logging.info("wsdl is at: http://localhost:8000/?wsdl")

    server.serve_forever()
