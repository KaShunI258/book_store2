import pytest
import time

from fe.access.new_buyer import register_new_buyer
from fe.test.gen_book_data import GenBook
import uuid


class TestAutoCancel:
    @pytest.fixture(autouse=True)
    def pre_run_initialization(self):
        self.seller_id = "test_new_order_seller_id_{}".format(str(uuid.uuid1()))
        self.store_id = "test_new_order_store_id_{}".format(str(uuid.uuid1()))
        self.buyer_id = "test_new_order_buyer_id_{}".format(str(uuid.uuid1()))
        self.password = self.seller_id
        self.buyer = register_new_buyer(self.buyer_id, self.password)
        self.gen_book = GenBook(self.seller_id, self.store_id)
        yield

    def test_auto_cancel(self):
        ok, buy_book_id_list = self.gen_book.gen(
            non_exist_book_id=False, low_stock_level=False
        )
        assert ok
        code, order_id = self.buyer.new_order(self.store_id, buy_book_id_list)
        assert code == 200

        time.sleep(30)

        # should be already canceled
        code = self.buyer.cancel_order(order_id)
        assert code != 200


    def test_normal_cancel(self):
        ok, buy_book_id_list = self.gen_book.gen(
            non_exist_book_id=False, low_stock_level=False
        )
        assert ok
        code, order_id = self.buyer.new_order(self.store_id, buy_book_id_list)
        assert code == 200

        time.sleep(5)

        code = self.buyer.cancel_order(order_id)
        assert code == 200


    def test_cancel_after_pay(self):
        ok, buy_book_id_list = self.gen_book.gen(
            non_exist_book_id=False, low_stock_level=False
        )
        assert ok
        code, order_id = self.buyer.new_order(self.store_id, buy_book_id_list)
        assert code == 200

        code = self.buyer.add_funds(99999999)
        assert code == 200

        code = self.buyer.payment(order_id)
        assert code == 200

        time.sleep(30)
        # can cancel
        code = self.buyer.cancel_order(order_id)
        assert code == 200
