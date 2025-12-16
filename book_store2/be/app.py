# import os
# os.chdir('E:\\juypter\\DataSql\\bookstore\\CDMS.Xuan_ZHOU.2024Fall.DaSE\\project1\\bookstore')
import os
import sys
# current_path = os.getcwd()
# print("current_path:", current_path)
sys.path.append("E:\\juypter\\DataSql\\bookstore2\\project1\\bookstore")

os.chdir('E:\\juypter\\DataSql\\bookstore2\\project1\\bookstore')
from be import serve
if __name__ == "__main__":
    # current_path = os.getcwd()
    # print("current_path:", current_path)
    serve.be_run()
#coverage run --source=fe -m pytest ./fe/test/test_add_book.py
#coverage run --timid --branch --source fe,be --concurrency=thread -m pytest -v --ignore=fe/data