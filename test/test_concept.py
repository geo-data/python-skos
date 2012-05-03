# -*- coding: utf-8 -*-

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import skos
import unittest

class TestCase(unittest.TestCase):
    """
    A base class used for testing schema objects
    """

    def setUp(self):
        # set up the database engine and the database schema
        self.engine = create_engine('sqlite:///:memory:')
        self.Session = sessionmaker(self.engine)
        self.createDbSchema()
        self.obj = self.getTestObj()

    def getChildConcepts(self):
        """
        Return a collection of concepts
        """

        return skos.Concepts([
                skos.Concept('uri1', 'prefLabel1', 'definition1'),
                skos.Concept('uri2', 'prefLabel2', 'definition2')
            ])

    def createDbSchema(self):
        session = self.Session()
        session.begin(subtransactions=True)

        conn = session.connection()
        skos.Base.metadata.create_all(conn)

        session.commit()

class TestConcept(TestCase):
    """
    A base class used for testing `Concept` objects
    """

    def getTestObj(self):
        return skos.Concept('uri', 'prefLabel', 'definition')

    def testInsert(self):
        session1 = self.Session()
        session2 = self.Session()

        # add the object to session1
        session1.begin(subtransactions=True)
        session1.add(self.obj)
        session1.commit()

        # query on session2 so we know we're using an object created
        # from scratch by sqlalchemy, not just returned from the
        # session1 cache.
        results = list(session2.query(skos.Concept).filter_by(uri=self.obj.uri))
        self.assertEqual(len(results), 1)
        concept = results[0]
        self.assertEqual(self.obj, concept)

    def testSynonyms(self):
        session1 = self.Session()
        session2 = self.Session()

        child_concepts = self.getChildConcepts()
        self.obj.synonyms = child_concepts

        # check the backreferencing is working
        for concept in child_concepts.itervalues():
            self.assertIn(concept, self.obj.synonyms)

        # add the object to session1
        session1.begin(subtransactions=True)
        session1.add(self.obj)
        session1.commit()

        # query on session2 so we know we're using an object created
        # from scratch by sqlalchemy, not just returned from the
        # session1 cache.
        results = list(session2.query(skos.Concept).filter_by(uri=self.obj.uri))
        self.assertEqual(len(results), 1)
        concept = results[0]
        self.assertEqual(self.obj, concept)
        self.assertEqual(self.obj.synonyms, concept.synonyms)

    def testRelated(self):
        session1 = self.Session()
        session2 = self.Session()

        child_concepts = self.getChildConcepts()
        self.obj.related = child_concepts

        # check the backreferencing is working
        for concept in child_concepts.itervalues():
            self.assertIn(concept, self.obj.related)

        # add the object to session1
        session1.begin(subtransactions=True)
        session1.add(self.obj)
        session1.commit()

        # query on session2 so we know we're using an object created
        # from scratch by sqlalchemy, not just returned from the
        # session1 cache.
        results = list(session2.query(skos.Concept).filter_by(uri=self.obj.uri))
        self.assertEqual(len(results), 1)
        concept = results[0]
        self.assertEqual(self.obj, concept)
        self.assertEqual(self.obj.related, concept.related)

    def testBroadness(self):
        session1 = self.Session()
        session2 = self.Session()

        # add some narrow matches to this broader concept
        child_concepts = self.getChildConcepts()
        self.obj.narrower = child_concepts

        # add the object to session1
        session1.begin(subtransactions=True)
        session1.add(self.obj)
        session1.commit()

        # query on session2 so we know we're using an object created
        # from scratch by sqlalchemy, not just returned from the
        # session1 cache.
        results = list(session2.query(skos.Concept).filter_by(uri=self.obj.uri))
        self.assertEqual(len(results), 1)
        concept = results[0]
        self.assertEqual(self.obj, concept)
        self.assertEqual(self.obj.narrower, concept.narrower)

        # check the backreferencing is working
        for child in concept.narrower.itervalues():
            self.assertIn(concept, child.broader)

if __name__ == '__main__':
    unittest.main(verbosity=2)
