from pymongo import MongoClient
from be.model import db_conn
from table_info import *

class BookSearcher(db_conn.DBConn):
    def __init__(self):
        db_conn.DBConn.__init__(self)
        self.client = MongoClient('localhost', 27017)
        self.mongo_db = self.client['bookstore']
    def __del__(self):
        self.close()
        self.client.close()
    def search_title_in_store(self, title: str, store_id: str, page_num: int, page_size: int):
        sql = """
        SELECT title, author, publisher, book_intro, tags
        FROM book 
        WHERE book_id IN 
        (SELECT book_id FROM search_title WHERE title = :title)
        AND book_id IN 
        (SELECT book_id FROM store WHERE store_id = :store_id)
        LIMIT :limit OFFSET :offset
        """
        offset = (page_num - 1) * page_size
        limit = page_size
        
        # 执行查询
        books = self.session.execute(
            text(sql), 
            {"title": title,"store_id":store_id, "limit": limit, "offset": offset}
        ).fetchall()

        # 返回结果
        if not books:
            return 501, f"for title:{title}, book not exist", []
        return 200, "ok", books
    
    def search_title(self, title: str, page_num: int, page_size: int):
        sql = """
        SELECT title, author, publisher, book_intro, tags
        FROM book 
        WHERE book_id IN 
        (SELECT book_id FROM search_title WHERE title = :title)
        LIMIT :limit OFFSET :offset
        """
        # 使用全文检索来搜索书籍
        offset = (page_num - 1) * page_size
        limit = page_size
       
        books = self.session.execute(
            text(sql), 
            {"title": title, "limit": limit, "offset": offset}
        ).fetchall()
        
        # 返回结果
        if not books:
            return 501, f"for title:{title}, book not exist", []
        return 200, "ok", books

    def search_tag_in_store(self, tag: str, store_id: str, page_num: int, page_size: int):
        # ret = []
        # if page_num < 1:
        #     return 200, []
        # records = self.session.execute(
        #     "SELECT title,author,publisher,book_intro,tags "
        #     "FROM book WHERE book_id in "
        #     "(select book_id from search_tags where tags='%s') and "
        #     "book_id in (select book_id from store where store_id='%s') LIMIT 10 OFFSET %d"
        #     % (tag, store_id, page_size)).fetchall()
        # if len(records) != 0:
        #     for i in range(len(records)):
        #         record = records[i]
        #         title = record[0]
        #         author = record[1]
        #         publisher = record[2]
        #         book_intro = record[3]
        #         tags_ = record[4]
        #         ret.append(
        #             {'title': title, 'author': author, 'publisher': publisher,
        #              'book_intro': book_intro,
        #              'tags': tags_})
        #     return 200, ret
        # else:
        #     return 200, []
        
        sql = """
        SELECT title, author, publisher, book_intro, tags
        FROM book 
        WHERE book_id IN 
        (SELECT book_id FROM search_tags WHERE tags = :tag)
        AND book_id IN 
        (SELECT book_id FROM store WHERE store_id = :store_id)
        LIMIT :limit OFFSET :offset
        """

        # debug = self.session.query(Store).filter(Store.store_id == store_id).all()
        
        
        offset = (page_num - 1) * page_size
        limit = page_size
        
        books = self.session.execute(
            text(sql), 
            {"tag": tag,"store_id":store_id, "limit": limit, "offset": offset}
        ).fetchall()
        
        if not books:
            return 501, f"for tag:{tag}, book not exist", []
        return 200, "ok", books

    
    def search_tag(self, tag: str, page_num: int, page_size: int):
        sql = """
        SELECT title, author, publisher, book_intro, tags
        FROM book 
        WHERE book_id IN 
        (SELECT book_id FROM search_tags WHERE tags = :tag)
        LIMIT :limit OFFSET :offset
        """
        offset = (page_num - 1) * page_size
        limit = page_size
        
        books = self.session.execute(
            text(sql), 
            {"tag": tag, "limit": limit, "offset": offset}
        ).fetchall()
        
        

        if not books:
            return 501, f"for tag:{tag}, book not exist", []
        return 200, "ok", books

    # def search_content_exactly_in_store(self, content: str, store_id: str, page_num: int, page_size: int):


    # 精确查询
    def search_content_in_store(self, content: str, store_id: str, page_num: int, page_size: int):
        # debug = self.mongo_db.book_content.find().sort({ "_id": 1 }).limit(10)
        # #print(debug)
        # for doc in debug:
        #     print(doc)
        condition = {"$text": {"$search": content}}
        books =list(self.mongo_db.book_intro.find(condition, {"_id": 0}).skip((page_num - 1) * page_size).limit(page_size))
        # res = []
        # for book in books:
        #     res.append(book)
        # 如果指定店铺ID，则只返回该店铺内的书籍
        if store_id:
            # store_collection = self.db.store
            books = [
                book for book in books
                if self.book_id_exist(store_id=store_id,book_id=book.get('id'))
                # if store_collection.count_documents({"store_id": store_id, "book_stock_info.book_id": book.get('id')}) > 0
            ]

        if not books:
            return 501, f"for content:{content},book not exist", []
        return 200, "ok", books

    def search_content(self, content: str, page_num: int, page_size: int):
        return self.search_content_in_store(content, "", page_num, page_size)

    
    def search_catalog(self, keyword, store_id=None, page=1, per_page=10):
        try:
            query = {"$text": {"$search": keyword}}
            result = (
                self.mongo_db.book_content.find(query, {"score": {"$meta": "textScore"}, "_id": 0, "picture": 0})
                .sort("score", {"$meta": "textScore"})
                .skip((page - 1) * per_page)
                .limit(per_page)
            )
            
            if store_id:
                result = [
                    book for book in result
                    if self.book_id_exist(store_id=store_id,book_id=book.get('id'))
                    # if store_collection.count_documents({"store_id": store_id, "book_stock_info.book_id": book.get('id')}) > 0
                ]
            return 200, list(result)
        
        except Exception as e:
            return 530, str(e)

    def search_author_in_store(self, author: str, store_id: str, page_num: int, page_size: int):
        sql = """
        SELECT title, author, publisher, book_intro, tags
        FROM book 
        WHERE book_id IN 
        (SELECT book_id FROM search_author WHERE author = :author)
        AND book_id IN 
        (SELECT book_id FROM store WHERE store_id = :store_id)
        LIMIT :limit OFFSET :offset
        """
        offset = (page_num - 1) * page_size
        limit = page_size
        
        result = self.session.execute(
            text(sql), 
            {"author": author, "store_id":store_id, "limit": limit, "offset": offset}
        ).fetchall()
        
        if len(result) == 0:
            return 501, f"for author:{author}, book not exist", []
            
        return 200, "ok", result
    
    def search_author(self, author: str, page_num: int, page_size: int):
        sql = """
        SELECT title, author, publisher, book_intro, tags
        FROM book 
        WHERE book_id IN 
        (SELECT book_id FROM search_author WHERE author = :author)
        LIMIT :limit OFFSET :offset
        """
        
        offset = (page_num - 1) * page_size
        limit = page_size

        result = self.session.execute(
            text(sql), 
            {"author": author, "limit": limit, "offset": offset}
        ).fetchall()
        
        if len(result) == 0:
            return 501, f"for author:{author}, book not exist", []
        return 200, "ok", result