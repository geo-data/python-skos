# -*- coding: utf-8 -*-

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import skos
from test_concept import TestCase

class TestCollection(TestCase):
    """
    A base class used for testing `Collection` objects
    """

    def getTestObj(self):
        return skos.Collection('uri', 'title', 'description')

    def testEqual(self):
        collection = self.getTestObj()
        self.assertEqual(self.obj, collection)

        collection.members = self.getChildConcepts()
        self.obj.members = self.getChildConcepts()
        self.assertEqual(self.obj, collection)
    
    def testInsert(self):
        session1 = self.Session()
        session2 = self.Session()

        self.obj.members = self.getChildConcepts()
        
        # add the object to session1
        session1.begin(subtransactions=True)
        session1.add(self.obj)
        session1.commit()

        # query on session2 so we know we're using an object created
        # from scratch by sqlalchemy, not just returned from the
        # session1 cache.
        results = list(session2.query(skos.Collection).filter_by(uri=self.obj.uri))
        self.assertEqual(len(results), 1)
        collection = results[0]
        self.assertEqual(self.obj, collection)
        self.assertEqual(len(collection.members), 2)

if __name__ == '__main__':
    import unittest
    unittest.main(verbosity=2)
