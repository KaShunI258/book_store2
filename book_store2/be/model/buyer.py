import logging
import threading
import uuid
import time
from datetime import datetime, timedelta
from be.model import db_conn
from be.model import error
from be.model.book_searcher import BookSearcher
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from table_info import *

class Buyer(db_conn.DBConn):
    def __init__(self):
        super().__init__()
        self.cleanup_thread = None
        self.is_running = False
        # 启动后台清理任务
        self.start_cleanup_thread()
    def __del__(self):
        self.stop_cleanup_thread()
        self.close()

    def new_order(self, user_id: str, store_id: str, id_and_count: [(str, int)]) -> (int, str, str):
        order_id = ""

        try:
            # 检查用户是否存在
            if not self.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id) + (order_id,)

            # 检查商店是否存在
            if not self.store_id_exist(store_id):
                return error.error_non_exist_store_id(store_id) + (order_id,)

            uid = f"{user_id}_{store_id}_{str(uuid.uuid1())}"

            # 处理每本书
            for book_id, count in id_and_count:
                #self.session.begin()
                store_detail = self.get_book_in_store_ex(store_id=store_id,book_id=book_id)
                if store_detail is None:
                    self.session.rollback()
                    return error.error_non_exist_book_id(book_id) + (order_id,)

                stock_level = store_detail.stock_level
                price = store_detail.price
                if stock_level < count:
                    self.session.rollback()
                    return error.error_stock_level_low(book_id) + (order_id,)

                # 更新库存
                store_detail.stock_level -= count
                self.session.add(Order_detail(order_id=uid, book_id=book_id, count=count,price = price))#price means one book
                
            # 创建订单
            self.session.add(Order(order_id=uid, user_id=user_id, store_id=store_id, create_time=datetime.now(),books_status = 2))
            self.session.commit()
            order_id = uid

        except Exception as e:
            self.session.rollback()
            return 528, f"{str(e)}", order_id
        return 200, "ok", order_id

    
    def payment(self, user_id: str, password: str, order_id: str) -> (int, str):
        try:
            # 查询订单
            order_info = self.session.query(Order).filter(Order.order_id == order_id).first()
            if order_info is None:
                return error.error_invalid_order_id(order_id)
            
            buyer_id = order_info.user_id
            store_id = order_info.store_id

            if buyer_id != user_id:
                return error.error_authorization_fail()

            if order_info.books_status != 2:  # 只允许未支付的订单付款
                return error.error_repeated_payment(order_id)

            # 验证用户密码
            user_info = self.get_user_by_id_ex(user_id=user_id)#也是buyer
            if user_info is None:
                return error.error_non_exist_user_id(buyer_id)
            if password != user_info.password:
                return error.error_authorization_fail()
            store_info = self.get_store_for_read(store_id=store_id)
            if  store_info is None:
                return error.error_non_exist_store_id(store_id)
            seller_id = store_info.user_id
            if not self.user_id_exist(seller_id):
                return error.error_non_exist_user_id(seller_id)

            order_details = self.session.query(Order_detail).filter(Order_detail.order_id == order_id).all()
            total_price = sum(detail.price * detail.count for detail in order_details)

            if user_info.balance < total_price:
                return error.error_not_sufficient_funds(order_id)

            # 更新余额
            user_info.balance -= total_price
            seller_info = self.get_user_by_id_ex(user_id=seller_id)
            seller_info.balance += total_price

            # 更新订单状态为已支付
            order_info_new = self.session.query(Order).filter(Order.order_id == order_id).with_for_update().first()
            if order_info_new.books_status != 2:  # 只允许未支付的订单付款
                return error.error_repeated_payment(order_id)
            order_info_new.books_status = 1
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            return 528, f"{str(e)}"

        return 200, "ok"

    def add_funds(self, user_id, password, add_value) -> (int, str):
        try:
            user_info = self.get_user_by_id_ex(user_id=user_id)
            if user_info is None or user_info.password != password:
                return error.error_authorization_fail()

            # 增加余额
            user_info.balance += add_value
            self.session.commit()

        except Exception as e:
            self.session.rollback()
            return 528, f"{str(e)}"
        except BaseException as e:
            return 530, "{}".format(str(e))

        return 200, "ok"

    def receive_book(self, user_id: str, order_id: str) -> (int, str):

        try:
            order_info = self.session.query(Order).filter(Order.order_id == order_id).first()
            if order_info is None:
                return error.error_invalid_order_id(order_id)

            if order_info.user_id != user_id:
                return error.error_authorization_fail()

            if order_info.books_status == 1:
                return error.error_books_not_sent(order_id)  # 订单已支付但未发货
            if order_info.books_status == 2:
                return error.error_books_receive_without_payment(order_id)  # 未支付无法收货
            if order_info.books_status == 3:
                return error.error_books_repeat_receive(order_id)  # 已经收货

            # 更新订单状态为已收货
            # 对订单加锁并更新状态
            locked_order = (
                self.session.query(Order)
                .filter(Order.order_id == order_id, Order.books_status == order_info.books_status)
                .with_for_update()
                .first()
            )
            if not locked_order:
                return 528, "Order status changed, please retry"

            # 更新订单状态为已收货
            locked_order.books_status = 3
            self.session.commit()

        except Exception as e:
            self.session.rollback()
            return 528, f"{str(e)}"

        return 200, "ok"

        # 用户查询历史订单

    def search_order(self, user_id: str, password: str) -> (int, str, [(str, str, str, int, int, int)]):
        try:
            # find user
            user_info = self.get_user_by_id_norm(user_id=user_id)
            if user_info is None or user_info.password != password:
                return error.error_authorization_fail() + ([])

            # find order
            #TODO :OPTIMIZE
            res = self.session.query(Order).filter(Order.user_id == user_id).with_for_update(read=True).all()
            # res = self.db.new_order.find({"user_id": user_id})
            order_list = {}

            # no orders，return empty list, message to hint no orders
            if res is None or len(res) == 0:
                return 200, "no orders", []

            # find order detail
            for row in res:
                order_id = row.order_id
                store_id = row.store_id
                status = row.books_status
                order_de = self.get_orders_detail_by_id_norm(order_id=row.order_id)
                order_id_details = []
                # find book
                for order_detail in order_de:
                    book_id = order_detail.book_id
                    count = order_detail.count
                    price = order_detail.price
                    order_id_details.append((store_id, book_id, count, price, status))
                order_list[order_id] = order_id_details

        except BaseException as e:
            return 528, "{}".format(str(e)), []

        return 200, "ok", order_list

    # 用户登录并主动取消订单，需要分为未付款的订单和已付款的订单待发货的，已发货或已收货不可以取消
    # 对于前者，只需要返还书籍数量即可，对于后者，需要返还书籍数量并且返还金额，返还金额需要从商家账户扣除

    def cancel_order(self, user_id: str, password: str, order_id: str) -> (int, str):
        try:
            # find user
            user_info = self.get_user_by_id_norm(user_id=user_id)#无锁
            if user_info is None or user_info.password != password:
                return error.error_authorization_fail()

            # find order
            order = self.get_one_order_by_id(order_id=order_id)#写锁
            # res = self.db.new_order.find_one({"order_id": order_id})
            if order == None:
                return error.error_invalid_order_id(order_id)

            # check order status
            status = order.books_status
            store_id = order.store_id
            order_details_list = self.get_orders_detail_by_id(order_id=order_id)#写锁

            if status == 2:
                # 返还书籍
                for order_detail in order_details_list:
                    book_id = order_detail.book_id
                    count = order_detail.count
                    store_stock = self.get_book_in_store_ex(store_id=store_id,book_id=book_id)
                    store_stock.stock_level += count
                    
                self.session.delete(order)
                for order_detail in order_details_list:
                    self.session.delete(order_detail)

            elif status == 1:
                # 检查商家余额是否足够退款
                store_info = self.get_store(store_id=store_id)
                # store_info = self.db.user_store.find_one({"store_id": store_id})
                seller_id = store_info.user_id

                total_price = sum(detail.price * detail.count for detail in order_details_list)

                 # 返还书籍
                for order_detail in order_details_list:
                    book_id = order_detail.book_id
                    count = order_detail.count
                    # 更新库存
                    store_stock = self.get_book_in_store(store_id=store_id,book_id=book_id)
                    store_stock.stock_level += count

                # 返还金额
                seller_info = self.get_user_by_id(seller_id)
                if seller_info.balance < total_price:
                    return error.error_not_sufficient_funds(order_id)
                
                user_info = self.get_user_by_id_ex(user_id)
                user_info.balance += total_price
                seller_info.balance -= total_price
                # 删除订单和订单详情
                self.session.delete(order)
                for order_detail in order_details_list:
                    self.session.delete(order_detail)
            elif status == 0:
                return error.error_book_has_sent(order_id)
            elif status == 3:
                return error.error_book_has_received(order_id)
            self.session.commit()
        except BaseException as e:
            return 528, "{}".format(str(e))
        return 200, "ok"


    def start_cleanup_thread(self):
        self.is_running = True
        self.cleanup_thread = threading.Thread(target=self._cleanup_expired_orders, daemon=True)
        self.cleanup_thread.start()
        logging.info("Started order cleanup thread")

    def stop_cleanup_thread(self):
        self.is_running = False
        if self.cleanup_thread:
            self.cleanup_thread.join()
            logging.info("Stopped order cleanup thread")

    def _cleanup_expired_orders(self):
        session = self.Session()
        while self.is_running:
            try:
                expire_time = datetime.now() - timedelta(seconds=20)

                expired_orders = session.query(Order).filter(Order.create_time < expire_time,
                                                                  Order.books_status == 2).with_for_update().all()
                
                expired_order_ids = [order.order_id for order in expired_orders]
                session.query(Order_detail).filter(
                        Order_detail.order_id.in_(expired_order_ids)
                    ).delete(synchronize_session=False)

                    # 批量删除订单
                session.query(Order).filter(
                        Order.order_id.in_(expired_order_ids)
                    ).delete(synchronize_session=False)
                session.commit()

            except Exception as e:
                session.rollback()
            # 每10秒检查一次
            time.sleep(10)


    # def search(self, keyword, store_id=None, page=1, per_page=10):
    #     try:
    #         # 全文搜索
    #         query = {"$text": {"$search": keyword}}

    #         # 如果指定了 store_id，则限定书籍范围
    #         if store_id:
    #             # 提取指定商店中的 book_id 列表
    #             books_id = [
    #                 book["book_id"] 
    #                 for store in self.db.store.find({"store_id": store_id}, {"book_stock_info.book_id": 1, "_id": 0})
    #                 for book in store.get("book_stock_info", [])
    #             ]
    #             query["id"] = {"$in": books_id}

    #         # 执行查询，排序，分页
    #         result = (
    #             self.db.book.find(query, {"score": {"$meta": "textScore"}, "_id": 0, "picture": 0})
    #             .sort("score", {"$meta": "textScore"})
    #             .skip((page - 1) * per_page)
    #             .limit(per_page)
    #         )
            
    #         return 200, list(result)
        
    #     except Exception as e:
    #         return 530, str(e)
