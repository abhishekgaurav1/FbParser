# -*- coding: UTF-8 -*-

'''
Created on 27-Sep-2015

@author: Abhishek.Gaurav
'''

import json
import requests
import MySQLdb

dbcon = MySQLdb.connect("localhost","","","")
dbcon.set_character_set('utf8')
dbcon.autocommit(True)
cur = dbcon.cursor()
cur.execute('SET NAMES utf8;')
cur.execute('SET CHARACTER SET utf8;')
cur.execute('SET character_set_connection=utf8;')
user_id="131183413901583"
f = open("fbcrawler.log","w+")
def get_group_post(group_name,group_id):
    """
        Returns group feed
    """
    try:
        url = 'https://graph.facebook.com/%s/feed'%group_id
        parameters = {'access_token': "",
                      }
        r = requests.get(url, params = parameters)
        result = json.loads(r.text)
        for i in result['data']:
            post_id = i.get("id","")
    #         user_name = i["from"]["name"].replace('"',"'").encode('ascii', 'xmlcharrefreplace')
    #         user_id = i["from"]["id"]
    #         group_name = i["to"]["data"][0]["name"].replace('"',"'").encode('ascii', 'xmlcharrefreplace')
    #         group_id = i["to"]["data"][0]["id"]
            message = i.get("message","").replace('"',"'").encode('ascii', 'xmlcharrefreplace')
            #description = i.get("description","").encode('ascii', 'xmlcharrefreplace')
            created_date = i["updated_time"].split("+")[0]
            if cur.execute("select id from fbdata1 where post_id = '%s'"%post_id):
                return
            query = u"""INSERT INTO `fbdata1` (`post_date`,`post_id`, `message`,`group_name`,`group_id`) VALUES \
            ('%s','%s',"%s","%s",'%s');"""% (created_date,post_id,message,group_name,group_id)
            try:
                cur.execute(query)            
            except Exception,e:
                f.write("Error in get group post - %s"%e)
        get_page_data(result["paging"]["next"],group_name,group_id)
        print "done!"
    except Exception,e:
        print e
        print result
def get_page_data(url,group_name,group_id):
    try:
        r = requests.get(url)
        result = json.loads(r.text)
        for i in result['data']:
            post_id = i.get("id","")
    #         user_name = i["from"]["name"].replace('"',"'").encode('ascii', 'xmlcharrefreplace')
    #         user_id = i["from"]["id"]
    #         group_name = i["to"]["data"][0]["name"].replace('"',"'").encode('ascii', 'xmlcharrefreplace')
    #         group_id = i["to"]["data"][0]["id"]
            message = i.get("message","").replace('"',"'").encode('ascii', 'xmlcharrefreplace')
            #description = i.get("description","").encode('ascii', 'xmlcharrefreplace')
            created_date = i["updated_time"].split("+")[0]
            if cur.execute("select id from fbdata1 where post_id = '%s'"%post_id):
                return
            query = u"""INSERT INTO `fbdata1` (`post_date`,`post_id`, `message`,`group_name`,`group_id`) VALUES \
            ('%s','%s',"%s","%s",'%s');"""% (created_date,post_id,message,group_name,group_id)
            try:
                cur.execute(query)            
            except Exception,e:
                print e
        if result["paging"].has_key("next") and result["paging"]["next"]:
            get_page_data(result["paging"]["next"],group_name,group_id)
        else:
            print "Over!"
    except Exception,e:
        f.write("Error in get page data - %s"%e)
        
data_dict = {'Bandra Proposals':'422831264520454','Real Estate Consultants in Mumbai':'233720376729232',
             'NOIDA - Shree Bhoopati Promoters Pvt Ltd':'221245877898358',
             'Pune Real Estate Group':'1485475111693737',
             'Real Property Agency':'344500165642305',
             'Real Estate Business':'604072516313542',
             'Dubai/Abu dhabi real estate investors forum ':'25762506353',
             'Real Estate Worldwide':'437118779752036',
             'Propertygarh.com':'301101019911764',
             'Real Estate Mumbai':'409878229071973',
             'Real Estate Agents Mumbai':'219443281402422'}

for name in data_dict.keys():    
    get_group_post(name,data_dict[name])
f.close()