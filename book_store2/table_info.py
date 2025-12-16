import re
import jieba
from sqlalchemy import Float, create_engine, text

from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String, Integer, ForeignKey, create_engine, Text, DateTime
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import os
import sqlite3 as sqlite
import simplejson as json
import jieba.analyse
#os.chdir('E:\\juypter\\DataSql\\bookstore2\\project1\\bookstore')

Base = declarative_base()

class User(Base):
    __tablename__ = 'user'
    user_id = Column(String(128), primary_key=True)
    password = Column(String(128), nullable=False)
    balance = Column(Integer, nullable=False)
    token = Column(String(4000), nullable=False)
    terminal = Column(String(256), nullable=False)


class UserStore(Base):
    __tablename__ = 'user_store'
    store_id = Column(String(128), primary_key=True)
    user_id = Column(String(128), ForeignKey('user.user_id'), nullable=False)


class Store(Base):
    __tablename__ = 'store'
    store_id = Column(String(128), ForeignKey('user_store.store_id'), primary_key=True)
    book_id = Column(String(128), ForeignKey('book.book_id'), primary_key=True)
    stock_level = Column(Integer, nullable=False)
    price = Column(Float,nullable= True)

class Book(Base):
    __tablename__ = 'book'
    book_id = Column(String(128), primary_key=True)
    title = Column(String, nullable=False)
    author = Column(String)
    publisher = Column(String)
    original_title = Column(Text)
    translator = Column(Text)
    pub_year = Column(Text)
    pages = Column(Integer)
    original_price = Column(Integer)
    currency_unit = Column(Text)
    binding = Column(Text)
    isbn = Column(Text)
    author_intro = Column(Text)
    book_intro = Column(String)
    # content = Column(Text)
    tags = Column(String)
    # picture = Column(LargeBinary)


class Order(Base):
    __tablename__ = 'order'
    order_id = Column(String(1280), primary_key=True)
    user_id = Column(String(128), ForeignKey('user.user_id'), nullable=False)
    store_id = Column(String(128), ForeignKey('user_store.store_id'), nullable=False)
    create_time= Column(DateTime, nullable=True)
    books_status = Column(Integer(), nullable=True)



class Order_detail(Base):
    __tablename__ = 'order_detail'
    order_id = Column(String(1280), primary_key=True, nullable=False)
    book_id = Column(String(128), ForeignKey('book.book_id'), primary_key=True, nullable=False)
    count = Column(Integer, nullable=False)
    price = Column(Float,nullable=False)


class Search_title(Base):
    __tablename__ = 'search_title'
    search_id = Column(Integer, primary_key=True, nullable=False)
    title = Column(String, primary_key=True, nullable=False)
    book_id = Column(String(128), ForeignKey('book.book_id'), nullable=False)


class Search_tags(Base):
    __tablename__ = 'search_tags'
    search_id = Column(Integer, primary_key=True, nullable=False)
    tags = Column(String, primary_key=True, nullable=False)
    book_id = Column(String(128), ForeignKey('book.book_id'), nullable=False)

class Search_author(Base):
    __tablename__ = 'search_author'
    search_id = Column(Integer, primary_key=True, nullable=False)
    author = Column(String, primary_key=True, nullable=False)
    book_id = Column(String(128), ForeignKey('book.book_id'), nullable=False)

class Search_book_intro(Base):
    __tablename__ = 'search_book_intro'
    search_id = Column(Integer, primary_key=True, nullable=False)
    book_intro = Column(String, primary_key=True, nullable=False)
    book_id = Column(String(128), ForeignKey('book.book_id'), nullable=False)


class Bookinit:
    id: str
    title: str
    author: str
    publisher: str
    original_title: str
    translator: str
    pub_year: str
    pages: int
    price: int
    binding: str
    isbn: str
    author_intro: str
    book_intro: str
    tags: [str]
    def __init__(self):
        self.tags = []
        self.pictures = []
