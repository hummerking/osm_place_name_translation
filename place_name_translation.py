#!/usr/bin/python
#coding:utf-8

import re
import psycopg2
import psycopg2.extras
import chardet
import sys 
reload(sys) 
sys.setdefaultencoding("utf-8")


def trans_name(name_ori):    
    global cur
    if name_ori:
        name_ori = name_ori.lower()
    cur.execute("""SELECT * FROM national_place_names WHERE lower_place_name = %s""", (name_ori,))
    row = cur.fetchone()
    if row and row['trans_name']:
        trans_name = row['trans_name'].split(',', 2)
        trans_name = trans_name[0].split('。', 2)
        return trans_name[0]

    return ''


def clear_name(name, name_en, name_zh):
    name = name.strip()  
    name_en = name_en.strip()
    name_zh = name_zh.strip()
    name_ori = ''
    name_ori_plus = ''
    
    if name.find(name_zh):
        return name

    if name == name_en:
        if name_zh:
            return name_en + " (" + name_zh + ")"
        else:
            name_trans_zh = trans_name(name_en)
            if name_trans_zh:
                return name_en + " (" + name_trans_zh + ")"


    m = re.match('(.*)\s*\((.*)\)\s*$', name)
    if not m:
        m = re.match('(.*)/(.*)', name)
    if m:
        name_ori = m.group(1).strip()
        name_ori_plus = m.group(2).strip()
        if len(name_ori_plus) > 1:
            name_trans_zh = trans_name(name_ori_plus)
            if name_trans_zh:
                return name_ori + " (" + name_trans_zh + ")"
            else:
                return name
        else:
            return name


    name_trans_zh = trans_name(name)
    if name_trans_zh:
        return name + " (" + name_trans_zh + ")"

    return name


def trans_db(dbname):
    global cur
    cur.execute("""SELECT osm_id, name, name_backup, tags->'name:en' as name_en, tags->'name:zh' as name_zh from """ + dbname + """ where name is not null""")
    rows = cur.fetchall()

    for row in rows:
        name = row['name'].strip()
        name_en = ''
        name_zh = ''
        if row['name_backup']:
            name = row['name_backup']
        if row['name_en']:
            name_en = row['name_en']
        if row['name_zh']:
            name_zh = row['name_zh']

        name_trans = clear_name(name, name_en, name_zh)
        if name_trans != name:
            if not row['name_backup']:
                cur.execute("""UPDATE """ + dbname + """ set name = %s, name_backup = %s WHERE osm_id = %s""", (name_trans, name, row['osm_id']))
            else:            
                cur.execute("""UPDATE """ + dbname + """ set name = %s WHERE osm_id = %s""", (name_trans, row['osm_id']))
            conn.commit()
            print dbname + "|" + str(row['osm_id']) + "|" + name + "|" + name_trans + "|"

try:
    conn = psycopg2.connect("dbname='osmgis' user='osmgis' host='localhost' password='******'")
except:
    print "I am unable to connect to the database"    

psycopg2.extras.register_hstore(conn)    
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

#planet_osm_point, planet_osm_polygon, planet_osm_line, planet_osm_roads
trans_db('planet_osm_roads')
