from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import joinedload
from sqlalchemy import and_, func
from be.model import error
from be.model import db_conn
from datetime import datetime
from table_info import *

class Seller(db_conn.DBConn):
    def __init__(self):
        db_conn.DBConn.__init__(self)
    # def __del__(self):
    #     # super().__delete__()
    #     self.close()
    def add_book(self, user_id: str, store_id: str, book_id: str, book_json_str: str, stock_level: int):
        try:
            # Check if the user exists
            if not self.user_id_exist(user_id=user_id):
                return error.error_non_exist_user_id(user_id)

            # Check if the store exists
            if not self.store_id_exist(store_id=store_id):
                return error.error_non_exist_store_id(store_id)

            # Check if the book already exists in the store
            #store_book = self.session.query(Store).join(Store.books).filter(Store.store_id == store_id, Store.book_id == book_id).first()
            
            if self.book_id_exist(store_id=store_id,book_id=book_id):
                return error.error_exist_book_id(book_id)

            # Update the store's stock level for the book
            price = self.get_book_price(book_id=book_id)
            new_store_book = Store(store_id=store_id, book_id=book_id, stock_level=stock_level,price = price )  # Assume price is 0 for now
            self.session.add(new_store_book)
            self.session.commit()

        except Exception as e:
            self.session.rollback()
            return 528, f"{str(e)}"
        except BaseException as e:
            self.session.rollback()
            return 530, f"{str(e)}"
        
        return 200, "ok"

    def add_stock_level(self, user_id: str, store_id: str, book_id: str, add_stock_level: int):
        try:
            # Check if the user exists
            #user = self.session.query(User).filter_by(user_id=user_id).first()
            if self.user_id_exist(user_id=user_id) is False:
                return error.error_non_exist_user_id(user_id)
            cursor = self.get_book_in_store_ex(store_id=store_id,book_id=book_id)
            if cursor is None:
                return error.error_non_exist_book_id(book_id)
            cursor.stock_level += add_stock_level
            self.session.commit()
            

        except Exception as e:
            self.session.rollback()
            return 528, f"{str(e)}"
        except BaseException as e:
            self.session.rollback()
            return 530, f"{str(e)}"

        return 200, "ok"

    def create_store(self, user_id: str, store_id: str) -> (int, str):
        try:
            # Check if the user exists
            if self.user_id_exist(user_id=user_id) is False:
                return error.error_non_exist_user_id(user_id)

            # Check if the store already exists
            if self.store_id_exist(store_id=store_id):
                return error.error_exist_store_id(store_id)

            # Create the new store
            new_store = UserStore(store_id=store_id, user_id=user_id)
            self.session.add(new_store)
            self.session.commit()

        except Exception as e:
            self.session.rollback()
            return 528, f"{str(e)}"
        except BaseException as e:
            self.session.rollback()
            return 530, f"{str(e)}"
        
        return 200, "ok"

    def send_books(self, user_id: str, order_id: str):
        try:
            # Fetch the order using order_id
            order = self.session.query(Order).filter_by(order_id=order_id).first()
            if not order:
                return error.error_invalid_order_id(order_id)

            store_id = order.store_id
            books_status = order.books_status

            # Check if the user is authorized
            store = self.session.query(UserStore).filter_by(store_id=store_id, user_id=user_id).first()
            if not store:
                return error.error_authorization_fail()

            # Check the status of the order
            if books_status == 0:
                return error.error_book_has_sent(order_id)
            if books_status == 2:
                return error.error_not_paid_book(order_id)
            if books_status == 3:
                return error.error_book_has_received(order_id)

            # Update the order status to "sent"
            with self.session.begin_nested():  # 开启事务，锁的范围仅限更新操作
                order = self.session.query(Order).filter_by(order_id=order_id).with_for_update().first()
                if not order:
                    return error.error_invalid_order_id(order_id)

                # 再次确认订单状态，以防在验证和加锁之间发生变化
                if order.books_status != 1:
                    return error.error_invalid_order_id(order_id)

                # 更新状态
                order.books_status = 0
            self.session.commit()

        except Exception as e:
            self.session.rollback()
            return 528, f"{str(e)}"
        except BaseException as e:
            self.session.rollback()
            return 530, f"{str(e)}"
        
        return 200, "ok"
