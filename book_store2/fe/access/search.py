import requests
import json

class Search:
    def __init__(self):
        self.url_prefix = "http://127.0.0.1:5000/search"

    def search_title(self, title):
        params = {
            "title": title
        }
        url = self.url_prefix + "/title"
        r = requests.get(url,params=params)
        res = json.loads(r.text)
        return res['code']

    def search_title_in_store(self, title, store_id):
        params = {
            "title": title,
            "store_id": store_id
        }

        url = self.url_prefix + "/title_in_store"

        r = requests.get(url,params=params)
        res = json.loads(r.text)
        return res['code']

    def search_tag(self, tag):
        params = {
            "tag": tag
        }

        url = self.url_prefix + "/tag"

        r = requests.get(url, params=params)
        res = json.loads(r.text)
        return res['code']

    def search_tag_in_store(self, tag, store_id):
        params = {
            "tag": tag,
            "store_id": store_id
        }
        url = self.url_prefix + "/tag_in_store"
        r = requests.get(url, params=params)
        res = json.loads(r.text)
        return res['code']

    def search_author(self, author):
        params = {
            "author": author
        }
        url = self.url_prefix + "/author"
        r = requests.get(url, params=params)
        res = json.loads(r.text)
        return res['code']

    def search_author_in_store(self, author, store_id):
        params = {
            "author": author,
            "store_id": store_id
        }
        url = self.url_prefix + "/author_in_store"
        r = requests.get(url, params=params)
        res = json.loads(r.text)
        return res['code']

    def search_content(self, content):
        params = {
            "content": content
        }
        url = self.url_prefix + "/content"
        r = requests.get(url, params=params)
        res = json.loads(r.text)
        return res['code']

    def search_content_in_store(self, content, store_id):
        params = {
            "content": content,
            "store_id": store_id
        }
        url = self.url_prefix + "/content_in_store"
        r = requests.get(url, params=params)
        res = json.loads(r.text)
        return res['code']
    
    def search_catalog(self, keyword, store_id=None, page=1):
        # store
        json = {
            "keyword": keyword,
            "page": page
        }
        if store_id:
            json["store_id"] = store_id

        url = self.url_prefix + "/catalog"
        r = requests.post(url, json=json)
        return r.content, r.status_code