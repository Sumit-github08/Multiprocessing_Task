# Make Folders named Processing,queue and processed and Write a code that makes a file(txt) every second in
#  the Processing folder, picks up all the files from processing and moves all the files to queue every 5
#   seconds and picks file from the queue folder and updates a column in MySQL/mongoDB table as 0/1 and
#    moves the file to the Processed folder. Also, make sure that no files are moved from Processing
#     to queue until the queue folder is empty.
import os
import time
import io
import shutil
import multiprocessing
import sqlite3
import pandas as pd


conn = sqlite3.connect('PreprocessingFiles.db')

c = conn.cursor()

c.execute("""CREATE TABLE folder (
            foldername,
            yes_no)""")


i=0

if not os.path.exists(os.getcwd()+"/processing"):
    os.mkdir(os.getcwd()+"/processing")

if not os.path.exists(os.getcwd()+ "/queue"):
    os.mkdir(os.getcwd()+"/queue")

if not os.path.exists(os.getcwd()+"/processed"):
    os.mkdir(os.getcwd()+"/processed")

def Processing():
    global i
    io.open(os.getcwd()+"\\processing\\file_"+str(i)+'.txt', 'w')
    i+=1
    print(f'Sleeping 1second...')
    time.sleep(1)

def Queue():
    if os.listdir("/queue") !='':
        for j in os.listdir("/processing"):
            shutil.move(os.getcwd()+ "\\processing\\"+j, os.getcwd()+"\\queue\\"+j)
    print(f'Sleeping 5 seconds...')
    time.sleep(5)

def insert_emp(foldername, yes_no):
    with conn:
        mycursor = conn.cursor()
        conn.execute("INSERT INTO folder VALUES (:foldername, :yes_no)", {'foldername': foldername, 'yes_no': yes_no})
        print(mycursor.rowcount, "was inserted.")

def Processed():
    if os.listdir("/queue")!= '':
        print("no. of files in queue:",len(os.listdir("/queue")))
        for j in os.listdir("/queue"):
            print(j)
            shutil.move(os.getcwd()+"\\queue\\"+j, os.getcwd()+ "\\processed\\"+j)
            #updating folder name and yes(1) if the folder moves to processed
            insert_emp(j, str(1))   


#mulitprocessing

p1= multiprocessing.Process(Processing)
p2= multiprocessing.Process(Queue)
p3= multiprocessing.Process(Processed)

p1.start()
p2.start()
p3.start()

p1.join()
p2.join()
p3.join()

print("Done!")