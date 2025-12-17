import json
import re
import pytest
import uuid
from fe.access.new_buyer import register_new_buyer
from fe.access.new_seller import register_new_seller
from fe.access.search import Search
from fe.access import book

class TestSearch:
    @pytest.fixture(autouse=True)
    def pre_run_initialization(self):
        self.buyer_id = "test_new_search_buyer_id_{}".format(str(uuid.uuid1()))
        self.password = "test_new_search_buyer_id_{}".format(str(uuid.uuid1()))
        self.buyer = register_new_buyer(self.buyer_id, self.password)

        self.user_id = "test_crete_store_user_{}".format(str(uuid.uuid1()))
        self.store_id = "test_create_store_store_{}".format(str(uuid.uuid1()))
        self.password = self.user_id
        self.seller = register_new_seller(self.user_id, self.password)
        code = self.seller.create_store(self.store_id)
        assert code == 200
        self.keyword = "hello"
        self.rs = Search()
        book_db = book.BookDB()
        self.book_example = book_db.get_book_info(0, 1)[0]


    def test_all_field_search(self):
        content, code = self.rs.search_catalog(self.keyword)
        # content, code = self.buyer.search(self.keyword)
        content = json.loads(content)['message']
        print(content, len(content))
        assert code == 200

    def test_pagination(self):
        content, code = self.rs.search_catalog(self.keyword, page=2)
        assert code == 200

    def test_search_title(self):

        code = self.seller.add_book(self.store_id, 0, self.book_example)
        assert code == 200

        code = self.rs.search_title(title=self.book_example.title)
        assert code == 200

        code = self.rs.search_title(title=self.book_example.title + str(uuid.uuid1()))
        assert code != 200

    def test_search_title_in_store(self):

        self.seller.add_book(self.store_id, 0, self.book_example)

        code = self.rs.search_title_in_store(title=self.book_example.title, store_id=self.store_id)
        assert code == 200

        code = self.rs.search_title_in_store(title=self.book_example.title + str(uuid.uuid1()),
                                                     store_id=self.store_id)
        assert code != 200

    def test_search_tag(self):

        self.seller.add_book(self.store_id, 0, self.book_example)
        tag = self.book_example.tags[0]
        code = self.rs.search_tag(tag=tag)
        assert code == 200

        code = self.rs.search_tag(tag=', '.join(tag) +str(uuid.uuid1()))
        assert code != 200

    def test_search_tag_in_store(self):

        self.seller.add_book(self.store_id, 0, self.book_example)
        tag = self.book_example.tags[0]
        code = self.rs.search_tag_in_store(tag=tag, store_id=self.store_id)
        assert code == 200

        code = self.rs.search_tag_in_store(tag=', '.join(tag) + str(uuid.uuid1()), store_id=self.store_id)
        assert code != 200

    def test_search_author(self):

        author = self.book_example.author
        author = re.sub(r'[\(\[\{（【][^)）】]*[\)\]\{\】\）]\s?', '', author)
        author = re.sub(r'[^\w\s]', '', author)
        self.seller.add_book(self.store_id, 0, self.book_example)

        code = self.rs.search_author(author=author)
        assert code == 200

        code = self.rs.search_author(author=author + str(uuid.uuid1()))
        assert code != 200

    def test_search_author_in_store(self):

        self.seller.add_book(self.store_id, 0, self.book_example)
        author = self.book_example.author
        author = re.sub(r'[\(\[\{（【][^)）】]*[\)\]\{\】\）]\s?', '', author)
        author = re.sub(r'[^\w\s]', '', author)
        code = self.rs.search_author_in_store(author=author, store_id=self.store_id)
        assert code == 200

        code = self.rs.search_author_in_store(author=author + str(uuid.uuid1()),
                                                      store_id=self.store_id)
        assert code != 200

    def test_search_content(self):

        self.seller.add_book(self.store_id, 0, self.book_example)
        key = self.book_example.book_intro
        code = self.rs.search_content(content=key)
        assert code == 200

        code = self.rs.search_content(content= "nonexistent_keyword_" + str(uuid.uuid4()))
        assert code != 200

    def test_search_content_in_store(self):

        self.seller.add_book(self.store_id, 0, self.book_example)
        key = self.book_example.book_intro
        code = self.rs.search_content_in_store(content=key, store_id=self.store_id)
        assert code == 200

        code = self.rs.search_content_in_store(content= "nonexistent_keyword_" + str(uuid.uuid4()),
                                                       store_id=self.store_id)
        assert code != 200

    