from lxml import html
import requests
import mysql.connector
from datetime import datetime, timedelta
import time
import traceback

class PlaylistEntry:
  def __init__(self, song, artist, airdate):
    self.song = song
    self.artist = artist
    self.airdate = airdate

class DBInfo:
    def __init__(self, database, cursor):
        self.database = database
        self.cursor = cursor

def logger(text):
    date = datetime.now()
    print("[" + date.strftime("%d.%m.%Y %H:%M") + "] " + text + ".")

def initOrSetDatabase():

    initDB = True

    database = mysql.connector.connect(
    host="127.0.0.1",
    port="3306",
    user="root",
    passwd="D5A\-$o{,B/r8$jPRkc]gz;H!."
    )

    dbinfo = DBInfo(database, database.cursor())
    
    dbinfo.cursor.execute("SHOW DATABASES")

    for x in dbinfo.cursor:
        if(str(x).find("radio") > 0):
            initDB = False
            #print("Database radio found.")

    if(initDB):
        print("Initializing database radio.")
        dbinfo.cursor.execute("CREATE DATABASE radio")
        dbinfo.cursor.execute("USE radio")
        dbinfo.cursor.execute("CREATE TABLE artist (ID MEDIUMINT NOT NULL AUTO_INCREMENT, name VARCHAR(250), PRIMARY KEY (ID) );")
        dbinfo.cursor.execute("CREATE TABLE song (ID MEDIUMINT NOT NULL AUTO_INCREMENT, title VARCHAR(250), PRIMARY KEY (ID) );")
        dbinfo.cursor.execute("CREATE TABLE playlist (ID MEDIUMINT NOT NULL AUTO_INCREMENT, artist_id MEDIUMINT, song_id MEDIUMINT, airdate DATETIME, PRIMARY KEY (ID) );")
        print("Database radio initialised.")

    dbinfo.cursor.execute("use radio")
    return dbinfo

def getPlaylistEntriesFromURL(date):

    playlist = []

    if(date > datetime.now()):
        return playlist

    songSearchUrl = 'https://www.rockantenne.de/musik/song-suche?station=rockantenne&date=' + str(date.day) + '.' + str(date.month) + '.' + str(date.year) + '&hour=' + str(date.hour) + '&minutes=' + date.strftime("%M") #Double zero

    page = requests.get(songSearchUrl)
    tree = html.fromstring(page.content)

    songs = tree.xpath('//h2[@class="song_title"]/a/text()')
    artists = tree.xpath('//p[@class="artist"]/a/text()')
    times = tree.xpath('//p[@class="artist"]/following::p[1]/text()')

    if(len(songs) != len(artists)):
        logger("Missmatch between songs and artist found on page")
        return playlist

    entryCount = 0
    for time in times:
        datetimestring = time[-23:]
        datestring = datetimestring[:10]
        timestring = datetimestring[-9:]
        timestring = timestring[:5]
        datetimestring = datestring + " " + timestring
        datetime_object = datetime.strptime(datetimestring, '%d.%m.%Y %H:%M')    
        
        plentry = PlaylistEntry(songs[entryCount].replace("'", ""), artists[entryCount].replace("'", ""), datetime_object)
        playlist.append(plentry)

        entryCount = entryCount + 1

    logger(str(len(playlist)) + " songs found for " + date.strftime("%d.%m.%Y %H:%M"))

    return playlist

def savePlaylistToDatabase(playlist, dbinfo):
    
    timescount = 0
    
    for entry in playlist:
        
        selstatement = "SELECT ID FROM playlist WHERE airdate = '" + str(entry.airdate) + "';"
        dbinfo.cursor.execute(selstatement)
        dbinfo.cursor.fetchall()

        artist_id = 0
        song_id = 0

        if(dbinfo.cursor.rowcount == 0):
        
            #Artist
            selectArtistID = "SELECT ID FROM artist WHERE name = '" + entry.artist + "';"
            dbinfo.cursor.execute(selectArtistID) 
            artistResList = dbinfo.cursor.fetchall()

            if(dbinfo.cursor.rowcount == 1):
                artist_id = str(artistResList[0])
                artist_id = artist_id.replace(")", "").replace("(", "").replace(",","")
                #print("Artist [" + cleanArtist + "] already listed with id [" + artist_id + "]")
                    
            else:         
                insertArtist = "INSERT INTO artist (name) VALUES ('" + entry.artist + "');"
                dbinfo.cursor.execute(insertArtist)
                dbinfo.database.commit()
                artist_id = dbinfo.cursor.lastrowid
                #print("Artist [" + cleanArtist + "] inserted with id [" + str(artist_id) + "]")

            #Song
            selectSondID = "SELECT ID FROM song WHERE title = '" + entry.song + "';"
            dbinfo.cursor.execute(selectSondID)
            songResList = dbinfo.cursor.fetchall()
            if(dbinfo.cursor.rowcount == 1):
                song_id = str(songResList[0]) #change to str(songResList[0][0]) and test
                song_id = song_id.replace(")", "").replace("(", "").replace(",","")
                #print("Song [" + cleanSong + "] already listed with id [" + song_id + "]")
            
            else:
                insertSong = "INSERT INTO song (title) VALUES ('" + entry.song + "');"
                dbinfo.cursor.execute(insertSong)
                dbinfo.database.commit()
                song_id = dbinfo.cursor.lastrowid
                #print("Song [" + cleanSong + "] inserted with id [" + str(song_id) + "]")

            #Playlist
            insertPlaylist = "INSERT INTO playlist (artist_id, song_id, airdate) VALUES (" + str(artist_id) + ", " + str(song_id) + ", '" + str(entry.airdate) + "');"
            dbinfo.cursor.execute(insertPlaylist)
            dbinfo.database.commit()
            logger(entry.song + " by " + entry.artist + " inserted at " + str(entry.airdate) + " with id " + str(dbinfo.cursor.lastrowid))

        timescount = timescount +1

def doEntriesExist(date, dbinfo):

    startDate = date.replace(minute = 0)
    endDate = date.replace(minute = 59)

    selstatement = "SELECT ID FROM playlist WHERE airdate BETWEEN '" + str(startDate) + "' AND '" + str(endDate) + "';"
    dbinfo.cursor.execute(selstatement)
    dbinfo.cursor.fetchall()

    if(dbinfo.cursor.rowcount > 0):
        return True
    
    return False        

###########
#  Start  #
###########

try:
    daysOfFuturePast = 20
    dbinfo = initOrSetDatabase()

    dayCount = 1
    while dayCount < daysOfFuturePast+1:
        subDate = datetime.today() - timedelta(days=dayCount)
        hours = range(0,24)
        minutes = [0,30]

        for hour in hours:
            
            subDate = subDate.replace(hour = hour)
            entriesExist = doEntriesExist(subDate, dbinfo)

            if entriesExist:
                logger("Skipped " + subDate.strftime("%d.%m.%Y %H:%M") + " while entries exist")
                continue

            for minute in minutes:
                subDate = subDate.replace(minute = minute)
                playlist = getPlaylistEntriesFromURL(subDate)
                if len(playlist) > 0:
                    savePlaylistToDatabase(playlist, dbinfo)
                #time.sleep(5) #DDOS delay ;)
            
        dayCount = dayCount +1
        
except Exception as e:
    logger("Unexpected error: " + traceback.format_exc())
    
