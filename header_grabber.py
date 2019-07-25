# -*- coding: utf-8 -*-
"""
Python Code using multiple threads to process the data. 

HK POLYU AFRO
Author: Peter Pham
"""

from urllib.request import urlopen 
from urllib.error import HTTPError
from urllib.error import URLError
from threading import Thread
import threading 
import queue  
from bs4 import BeautifulSoup 
import re
from  datetime import datetime 
import time

#Global Variables 
#Lock to Control the writing to the file
lock = threading.Lock() 
q = queue.Queue()
startTime = datetime.now()
output = open("header_data.csv", "w")

def  acc_num_grab(acc_file):
    match = re.search('(.*)\.txt',acc_file)
    if match: 
        return match.group(1)
    else: 
        return None
def url_create(line_array, acc_num):
    sec_base = "http://www.sec.gov/Archives/edgar/data/"
    acc_no_dash = re.sub("-","",acc_num)
    new_url = sec_base + line_array[0]+"/"+acc_no_dash+"/"+acc_num+".hdr.sgml"
    return new_url
    
def hdr_process():
    
    while not q.empty():
        data = q.get() # Getting Data from Queue
        acc_num = acc_num_grab(data[6])
        new_url = url_create(data,acc_num)
        lock.acquire()
        print (new_url)
        lock.release()
        
        
        try: 
            html = urlopen(new_url)
            for line in html: 
                match = re.search('<FILING-DATE>(.*)',str(line), re.I)
                if match: 
                    lock.acquire()
                    print (match.group(1))
                    print (datetime.now() - startTime)
                    lock.release() 
            print ("HTTPERROR\n")
        except AttributeError as e:
            print ("AttributeError\n")
        except URLError as e: 
            print ("URLError\n")
                   
        
        q.task_done()
    

def main():
    first_line = 1

    #Filename that can be changed 
    filename = "sec_cik.csv"
    
    #number of concurrent threads
    max_threads = 10 
    
    #opening the file and reading a line at a 
    for line in open (filename): 
        if first_line == 1:
            first_line = 0
            next 
        else:
            line_spl = line.split(',')
            q.put(line_spl)
           # acc_num = acc_num_grab(line_spl[6])
           # new_url = url_create(line_spl,acc_num)
           # print (new_url)
    
    #Initialize the threads
    for i in range (max_threads):
        t = Thread(target = hdr_process)
        t.start()
    

    q.join()
            

main()