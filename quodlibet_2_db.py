#!/usr/bin/env python2

username = ""
password = ""
database = "quodlibet"
quodlibet = ".quodlibet/songs"

import pickle
lib = pickle.load(open(quodlibet, "rb"))

import MySQLdb as mdb
import sys
db = mdb.connect('localhost', username, password, database)
try:
    c = db.cursor()
    c.execute('DELETE FROM songs;')
    c.execute('DELETE FROM albums;')
    c.execute('DELETE FROM artists;')

    artists = dict()
    albums = dict()
    songs = []

    artist_id = 0
    album_id = 0
    for song in lib:
        if song.has_key('artist') and not artists.has_key(song['artist']):
            artist_id += 1
            artists[song['artist']] = dict()
            artists[song['artist']]['id'] = artist_id
            artists[song['artist']]['artist'] = "'" + db.escape_string(song['artist']) + "'"
            artists[song['artist']]['artistsort'] = "'" + db.escape_string(song.get('artistsort')) + "'" if song.has_key('artistsort') else "NULL"
        if song.has_key('album') and not albums.has_key(song['album']):
            album_id += 1
            albums[song['album']] = dict()
            albums[song['album']]['id'] = album_id
            albums[song['album']]['album'] = "'" + db.escape_string(song['album']) + "'"
            albums[song['album']]['albumsort'] = "'" + db.escape_string(song.get('albumsort')) + "'" if song.has_key('albumsort') else "NULL"
            albums[song['album']]['date'] = "'" + db.escape_string(song.get('date')) + "'" if song.has_key('date') else "NULL"
            albums[song['album']]['genre'] = "'" + db.escape_string(song.get('genre')) + "'" if song.has_key('genre') else "NULL"
            albums[song['album']]['artist_id'] = artists[song['artist']]['id'] if song.has_key('artist') else "NULL"
            
        item = dict()
        item['title'] = "'" + db.escape_string(song['title']) + "'"
        item['tracknumber'] = "'" + db.escape_string(song.get('tracknumber')) + "'" if song.has_key('tracknumber') else "NULL"
        item['length'] = song['~#length']
        item['filename'] = "'" + db.escape_string(song['~filename']) + "'"
        item['album_id'] = albums[song['album']]['id'] if song.has_key('album') else "NULL"
        songs.append(item)

    for artist in artists:
        stmt = "INSERT INTO artists(id,artist,artistsort,created_at,updated_at) VALUES({0},{1},{2},NOW(),NOW())".format(artists[artist]['id'], artists[artist]['artist'], artists[artist]['artistsort'])
        print stmt
        c.execute(stmt)

    for album in albums:
        stmt = "INSERT INTO albums(id,album,albumsort,date,genre,artist_id,created_at,updated_at) VALUES({0},{1},{2},{3},{4},{5},NOW(),NOW())".format(albums[album]['id'], albums[album]['album'], albums[album]['albumsort'], albums[album]['date'], albums[album]['genre'], albums[album]['artist_id'])
        print stmt
        c.execute(stmt)

    for song in songs:
        stmt = "INSERT INTO songs(tracknumber,title,length,filename,album_id,created_at,updated_at) VALUES({0},{1},{2},{3},{4},NOW(),NOW())".format(song['tracknumber'], song['title'], song['length'], song['filename'], song['album_id'])
        print stmt
        c.execute(stmt)

    db.commit()

except mdb.Error, e:
    db.rollback()
    print "Error %d: %s" % (e.args[0],e.args[1])
    sys.exit(1)

finally:
    if c:
        c.close()
