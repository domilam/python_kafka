#!/usr/bin/env python3.5
import site
site.addsitedir('/home/dominique/etude/supervision/')

from mypymongo import MyPyMongo


class MongoUtils:
    def __init__(self):
        self._db = None
        self._con = None
        self._collection_company = 'companies'
        self._collection_user = 'users'
        self._collection_test = 'test'

    def create_company(self, company, db_auth):
        """
        create a company collection in mongo
        :param company:
        :param db_auth:
        :return:
        """
        mymongo = MyPyMongo()
        con_auth, db_auth = mymongo(db_auth)
        document_company = {'company': company}
        db_auth[self._collection_company].insert_one(document_company)
        con_auth.close()
        return document_company

    def create_user(self, user, db_auth):
        """
        create a user of a company
        :param user:
        :param db_auth:
        """
        mymongo = MyPyMongo()
        con_auth, db_auth = mymongo(db_auth)
        db_auth[self._collection_user].insert(user)
        con_auth.close()

    def create_db_company(self, company, db_auth_p):
        """
        create a company database in mongo
        :param company: 
        :param db_auth_p:
        """
        mymongo = MyPyMongo()
        con_auth, db_auth = mymongo(db_auth_p)
        print(db_auth)
        result = db_auth['companies'].find_one({'company': company})
        con, db = mymongo(company + '_ALOE_' + 'DB_' + str(result['_id']))
        document_db_company = {'test': 'test'}
        db[self._collection_test].insert_one(document_db_company)
        con.close()

if __name__ == '__main__':
    mymongoutils = MongoUtils()
    mymongoutils.create_db_company('test', 'aloe_auth_test')

