# -*- coding: utf-8 -*-

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import skos
from test_concept import TestCase

class TestConceptScheme(TestCase):
    """
    A base class used for testing `ConceptScheme` objects
    """

    def getTestObj(self):
        return skos.ConceptScheme('uri', 'title', 'description')

    def testInheritance(self):
        super(TestConceptScheme, self).doTestInheritance()

    def testEqual(self):
        scheme = self.getTestObj()
        self.assertEqual(self.obj, scheme)

        scheme.concepts = self.getChildConcepts()
        self.obj.concepts = self.getChildConcepts()
        self.assertEqual(self.obj, scheme)
    
    def testInsert(self):
        session1 = self.Session()
        session2 = self.Session()

        self.obj.concepts = self.getChildConcepts()
        
        # add the object to session1
        session1.begin(subtransactions=True)
        session1.add(self.obj)
        session1.commit()

        # query on session2 so we know we're using an object created
        # from scratch by sqlalchemy, not just returned from the
        # session1 cache.
        results = list(session2.query(skos.ConceptScheme).filter_by(uri=self.obj.uri))
        self.assertEqual(len(results), 1)
        scheme = results[0]
        self.assertEqual(self.obj, scheme)
        self.assertEqual(len(scheme.concepts), 2)

if __name__ == '__main__':
    from test import unittest
    unittest.main(verbosity=2)
