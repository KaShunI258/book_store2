from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text
from sqlalchemy import select
from sqlalchemy.orm import joinedload
from table_info import *

class DBConn:
    def __init__(self):
        self.engine = create_engine('postgresql://postgres:159753@localhost:5432/bookstore', pool_size=5000, max_overflow=500)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()
    # def __del__(self):
    #     self.session.close()
    def close(self):
        if self.session:
            self.session.close()
        if self.engine:
            self.engine.dispose()
    def __del__(self):
        self.close()

    # Check if user exists in PostgreSQL
    def user_id_exist(self, user_id):
        result = self.session.query(User).filter(User.user_id == user_id).with_for_update(key_share=True).first()
        return result is not None
    
    def get_user_by_id(self,user_id):
        result = self.session.query(User).filter(User.user_id == user_id).with_for_update(read=True).first()
        return result
    
    def get_user_by_id_norm(self, user_id: str):
        return self.session.query(User).filter(User.user_id == user_id).first()

    def get_user_by_id_ex(self,user_id):
        result = self.session.query(User).filter(User.user_id == user_id).with_for_update().first()
        return result
    # Check if book exists, with optional store filter
    def book_id_exist(self, store_id=None, book_id=None):
        if store_id is None or store_id == "":
            # Query the Book table directly
            result = self.session.query(Book).filter(Book.book_id == book_id).with_for_update(key_share=True).first()
        else:
            # Query the Store table and check if the book exists for the given store
            result = self.session.query(Store).filter(
                Store.store_id == store_id,
                Store.book_id == book_id
            ).with_for_update(key_share=True).first()
        return result is not None
    #for read
    def get_book_in_store(self,store_id = None,book_id = None):
        assert book_id is not None
        if store_id is None or store_id == "":
            # Query the Book table directly
            result = self.session.query(Book).filter(Book.book_id == book_id).with_for_update(read=True).first()
        else:
            # Query the Store table and check if the book exists for the given store
            result = self.session.query(Store).filter(
                Store.store_id == store_id,
                Store.book_id == book_id
            ).with_for_update(read=True).first()
        return result

    def get_book_in_store_ex(self,store_id = None,book_id = None):
        assert book_id is not None
        if store_id is None or store_id == "":
            # Query the Book table directly
            result = self.session.query(Book).filter(Book.book_id == book_id).with_for_update().first()
        else:
            # Query the Store table and check if the book exists for the given store
            result = self.session.query(Store).filter(
                Store.store_id == store_id,
                Store.book_id == book_id
            ).with_for_update().first()
        return result

    def get_store(self,store_id):
        result = self.session.query(UserStore).filter(UserStore.store_id == store_id).with_for_update().first()
        return result
    def get_store_for_read(self,store_id):
        result = self.session.query(UserStore).filter(UserStore.store_id == store_id).with_for_update(read=True).first()
        return result
        # Check if store exists in PostgreSQL
    def store_id_exist(self, store_id):
        result = self.session.query(UserStore).filter(UserStore.store_id == store_id).with_for_update(key_share=True).first()
        #result = self.get_store(store_id=store_id)
        return result is not None


    # Get the price of a book given its book_id
    def get_book_price(self, book_id):
        result = self.session.query(Book).filter(Book.book_id == book_id).with_for_update(read=True).first()
        if result:
            return result.original_price  # Assuming 'original_price' is the field for price in the Book model
        return None

    def get_orders_detail_by_id(self,order_id):
        res = self.session.query(Order_detail).filter(Order_detail.order_id == order_id).with_for_update().all()
        return res
    
    def get_orders_detail_by_id_norm(self,order_id):
        res = self.session.query(Order_detail).filter(Order_detail.order_id == order_id).all()
        return res
    def get_one_order_by_id(self,order_id):
        res = self.session.query(Order).filter(Order.order_id == order_id).with_for_update().first()
        return res
    

# # Usage example
# db_conn = DBConn()

# # Check if a user exists
# user_exists = db_conn.user_id_exist('some_user_id')
# print(user_exists)

# # Check if a book exists
# book_exists = db_conn.book_id_exist(book_id='some_book_id')
# print(book_exists)

# # Get store details
# store = db_conn.get_store(store_id='some_store_id')
# print(store)

# # Get book price
# book_price = db_conn.get_book_price(book_id='some_book_id')
# print(book_price)



# from be.model import store
# from pymongo import MongoClient

# class DBConn:
#     def __init__(self):
#         self.client = MongoClient('localhost', 27017)
#         self.db = self.client['bookstore']
#     def user_id_exist(self, user_id):
#         return self.db["user"].find_one({"user_id": user_id}) is not None


#     def book_id_exist(self, store_id = None, book_id = None):

#         assert book_id is not None

#         if store_id is None:
#             result = self.db["book"].find_one({"book_id": book_id})
#         else:
#             result = self.db["store"].find_one({
#                 "store_id": store_id,
#                 "book_stock_info.book_id": book_id
#             })
#             # result = self.db["book"].find_one({"store_id": store_id, "book_id": book_id})
#         return result is not None

#     # def book_id_exist(self,book_id):
#     #     result = self.db["book"].find_one({"book_id": book_id})
#     #     return result is not None
   

#     def store_id_exist(self, store_id):
#         result = self.db["user_store"].find_one({"store_id": store_id})
#         return result is not None

#     def get_store(self,store_id):
#         #debug
#         stores_cursor = self.db.store.find()
#         stores_list = list(stores_cursor)
#         return self.db.store.find_one({"store_id": store_id})
    
#     def get_book_price(self,book_id):
#         #line_from_store: self.db.store.find_one
#         res = self.db["book"].find_one({
#             "id":book_id
#         })
#         return res["price"]
