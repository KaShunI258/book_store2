# 测试用户查询历史订单

import pytest

from fe.access.new_buyer import register_new_buyer
from fe.access.buyer import Buyer
from fe.test.gen_book_data import GenBook
import uuid


class TestSearchOrder:
    # 需要测试 1.查找的用户没有任何订单 2.用户不存在或密码错误 3.正常情况
    user_id: str
    password: str
    buyer: Buyer
    store_id = "test_search_order_store_id_{}".format(str(uuid.uuid1()))
    seller_id = "test_search_order_seller_id_{}".format(str(uuid.uuid1()))
    gen_book: GenBook = GenBook(seller_id, store_id)

    @pytest.fixture(autouse=True)
    def pre_run_initialization(self):
        self.user_id = "test_search_order_user_id_{}".format(str(uuid.uuid1()))
        self.password = self.user_id
        self.buyer = register_new_buyer(self.user_id, self.password)
        yield


    def test_search_order_no_order(self):
        code = self.buyer.search_order()
        assert code == 200

    def test_search_order_authorization_error(self):
        self.buyer.password = self.buyer.password + "_x"
        code = self.buyer.search_order()
        assert code != 200

    def test_search_order_non_exist_user_id(self):
        self.buyer.user_id = self.buyer.user_id + "_x"
        code = self.buyer.search_order()
        assert code != 200

    def test_search_order_ok(self):
        ok, buy_book_id_list = self.gen_book.gen(
            non_exist_book_id=False, low_stock_level=False
        )
        assert ok
        code, _ = self.buyer.new_order(self.store_id, buy_book_id_list)
        assert code == 200
        code = self.buyer.search_order()
        assert code == 200

