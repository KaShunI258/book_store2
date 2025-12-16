import re
import jieba
from sqlalchemy import Float, create_engine, text
from pymongo import MongoClient

from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String, Integer, ForeignKey, create_engine, Text, DateTime
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import os
import sqlite3 as sqlite
import simplejson as json
import jieba.analyse

#os.chdir('E:\\juypter\\DataSql\\bookstore2\\project1\\bookstore')
from table_info import *

engine = create_engine('postgresql://postgres:159753@localhost:5432/bookstore', pool_size=5000, max_overflow=500)
DBSession = sessionmaker(bind=engine)
session = DBSession()

mongo_client = MongoClient('localhost', 27017) #store large file,such as picture,book content

with engine.connect() as conn:
    conn.execute(text("SET client_encoding TO 'gbk'"))

class BookDB:
    def __init__(self):
        self.book_db = "fe/data/book.db"

    def get_book_count(self):
        conn = sqlite.connect(self.book_db)
        cursor = conn.execute(
            "SELECT count(id) FROM book")
        row = cursor.fetchone()
        return row[0]

    def send_info_to_db(self, start, size):
        DBSession = sessionmaker(bind=engine)
        session = DBSession()

        mongo_db = mongo_client['bookstore']  
        book_picture = mongo_db['book_picture'] 
        # book_text = mongo_db['large_text']
        book_content = mongo_db['book_content']
        book_intro = mongo_db['book_intro']

        conn = sqlite.connect(self.book_db)
        cursor = conn.execute(
            "SELECT id, title, author, "
            "publisher, original_title, "
            "translator, pub_year, pages, "
            "price, currency_unit, binding, "
            "isbn, author_intro, book_intro, "
            "content, tags, picture FROM book ORDER BY id "
            "LIMIT ? OFFSET ?", (size, start))
        for row in cursor:
            book = Book()
            book.book_id = row[0]
            book.title = row[1]
            book.author = row[2]
            book.publisher = row[3]
            book.original_title = row[4]
            book.translator = row[5]
            book.pub_year = row[6]
            book.pages = row[7]
            book.original_price = row[8]
            book.currency_unit = row[9]
            book.binding = row[10]
            book.isbn = row[11]
            book.author_intro = row[12]
            book.book_intro = row[13]
            content = row[14]
            tags = row[15]
            picture = row[16]
            thelist = []
            for tag in tags.split("\n"):
                if tag.strip() != "":
                    thelist.append(tag)

            book.tags = str(thelist)

            session.add(book)

            book_content.insert_one({"id":row[0],
                            "content":content})
            book_intro.insert_one({"id":row[0],
                            "intro":book.book_intro})
            book_picture.insert_one({"id":row[0],
                            "picture":picture})
            # book_text.insert_one({"id":row[0],
            #                       "intro":book.book_intro,
            #                       "content":content})


        session.commit()
        session.close()
        
    def send_info(self):
        bookdb.send_info_to_db(0, bookdb.get_book_count())


def insert_tags():
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    row = session.execute(text("SELECT book_id, tags FROM book;")).fetchall()
    m = 0
    for i in row:
        tmp = i.tags.replace("'", "").replace("[", "").replace("]","").split(", ")
                                                               
        for j in tmp:
            session.execute(
                text("INSERT into search_tags(search_id, tags, book_id) VALUES (%d, '%s', %d)"
                % (m, j, int(i.book_id)))
            )
            m = m + 1
    session.commit()


def insert_author():
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    row = session.execute(text("SELECT book_id, author FROM book;")).fetchall()
    m = 0
    for i in row:
        tmp = i.author
        if tmp == None:
            j = 'unkonwn'
            session.execute(
                text("INSERT into search_author(search_id, author, book_id) VALUES (%d, '%s', %d)"
                % (m, j, int(i.book_id))))
            m = m + 1
        else:
            tmp = re.sub(r'[\(\[\{（【][^)）】]*[\)\]\{\】\）]\s?', '', tmp)
            tmp = re.sub(r'[^\w\s]', '', tmp)
            #length = len(tmp)
            session.execute(
                    text("INSERT into search_author(search_id, author, book_id) VALUES (%d, '%s', %d)"
                    % (m, tmp, int(i.book_id)))
                )
            m += 1
            # for k in range(1, length + 1):
            #     if tmp[k - 1] == '':
            #         continue
            #     j = tmp[:k]
            #     session.execute(
            #         text("INSERT into search_author(search_id, author, book_id) VALUES (%d, '%s', %d)"
            #         % (m, j, int(i.book_id)))
            #     )
            #     m = m + 1
    session.commit()


def insert_title():
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    row = session.execute(text("SELECT book_id, title FROM book;")).fetchall()
    m = 0
    for i in row:
        tmp = i.title
        tmp = re.sub(r'[\(\[\{（【][^)）】]*[\)\]\{\】\）]\s?', '', tmp)
        tmp = re.sub(r'[^\w\s]', '', tmp)
        if len(tmp) == 0:
            continue

        seg_list = jieba.cut_for_search(tmp)
        sig_list = []
        tag = 0
        for k in seg_list:
            sig_list.append(k)
            if k == tmp:
                tag = 1
        if tag == 0:
            sig_list.append(tmp)

        for j in sig_list:
            if j == "" or j == " ":
                continue
            session.execute(
                text("INSERT into search_title(search_id, title, book_id) VALUES (%d, '%s', %d)"
                % (m, j, int(i.book_id))))
            m = m + 1
    session.commit()


def insert_book_intro():
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    row = session.execute(text("SELECT book_id, book_intro FROM book;")).fetchall()
    m = 0
    for i in row:
        tmp = i.book_intro
        if tmp != None:
            keywords_textrank = jieba.analyse.textrank(tmp)
            for j in keywords_textrank:
                session.execute(
                    text("INSERT into search_book_intro(search_id, book_intro, book_id) VALUES (%d, '%s', %d)"
                    % (m, j, int(i.book_id))))
                m = m + 1
    session.commit()



def init_db():
    Base.metadata.create_all(engine)

#delete all table
def drop_db():
    Base.metadata.drop_all(engine)
    db_list = mongo_client.list_database_names()
    print(db_list)
    if 'bookstore' in db_list:
        mongo_client.drop_database('bookstore')
        print("In mongodb, Existing 'bookstore' database found and deleted.")


def create_index():
    mongo_db = mongo_client['bookstore'] 
    mongo_db['book_picture'].create_index([("picture", "text")]) 
    mongo_db['book_content'].create_index([("content", "text")]) 
    mongo_db['book_intro'].create_index([("intro", "text")]) 
    #mongo_db['large_text'].create_index([("intro", "text"),("content", "text")])
if __name__ == "__main__":
    drop_db()
    init_db()
    create_index()
    bookdb = BookDB()
    bookdb.send_info()
    insert_tags()
    insert_author()
    insert_title()
    insert_book_intro()
    session.close()