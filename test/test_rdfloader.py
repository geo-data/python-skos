# -*- coding: utf-8 -*-

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import skos
from test import unittest
import rdflib
import os.path
import datetime

class TestRDFUriNormalisation(unittest.TestCase):
    """
    Test type checking in constructor
    """

    def testContructorArguments(self):
        with self.assertRaises(TypeError):
            skos.RDFLoader('oops')

        graph = rdflib.Graph()
        with self.assertRaises(TypeError):
            skos.RDFLoader(graph, max_depth='oops')

        with self.assertRaises(TypeError):
            skos.RDFLoader(graph, normalise_uri='oops')

class TestCase(unittest.TestCase):
    
    def setUp(self):
        graph = rdflib.Graph()
        directory = os.path.dirname(__file__)
        graph.parse(os.path.join(directory, 'concepts.xml'))
        graph.parse(os.path.join(directory, 'schemes.xml'))
        self.loader = self.getLoader(graph)

class TestRDFLoader(TestCase):
    """
    A base class used for testing `RDFLoader` objects
    """

    def getLoader(self, graph):
        return skos.RDFLoader(graph, 0)
    
    def getKeys(self):
        return [
            'http://portal.oceannet.org/test',
            'http://portal.oceannet.org/test2',
            'http://portal.oceannet.org/test3',
            'http://portal.oceannet.org/collection',
            'http:/example.com/thesaurus'
            ]

    def testIn(self):
        self.assertIn('http://portal.oceannet.org/test', self.loader)

    def testIteration(self):
        received = list(iter(self.loader))
        received.sort()
        expected = self.getKeys()
        expected.sort()
        self.assertSequenceEqual(received, expected)

    def testLen(self):
        self.assertEqual(len(self.loader), 5)

    def testGetItem(self):
        value = self.loader['http://portal.oceannet.org/test']
        self.assertIsInstance(value, skos.Concept)

        value = self.loader['http:/example.com/thesaurus']
        self.assertIsInstance(value, skos.ConceptScheme)

    def testGetConcepts(self):
        concepts = self.loader.getConcepts()
        self.assertIsInstance(concepts, skos.Concepts)
        self.assertEqual(len(concepts), 3)
        for concept in concepts.itervalues():
            self.assertIsInstance(concept, skos.Concept)
            self.assertGreater(len(concept.uri), 1)
            self.assertGreater(len(concept.prefLabel), 1)
            self.assertGreater(len(concept.definition), 1)
            self.assertGreater(len(concept.notation), 1)

    def testGetConceptSchemes(self):
        schemes = self.loader.getConceptSchemes()
        self.assertIsInstance(schemes, skos.Concepts)
        self.assertEqual(len(schemes), 1)
        for scheme in schemes.itervalues():
            self.assertIsInstance(scheme, skos.ConceptScheme)

    def testGetCollections(self):
        collections = self.loader.getCollections()
        self.assertIsInstance(collections, skos.Concepts)
        self.assertEqual(len(collections), 1)
        for collection in collections.itervalues():
            self.assertIsInstance(collection, skos.Collection)
            self.assertGreater(len(collection.title), 1)
            self.assertGreater(len(collection.description), 1)
            self.assertIsInstance(collection.date, datetime.datetime)

    def testFlattening(self):
        self.loader.flat = True
        self.assertEqual(len(self.loader), 5)

class TestRDFParsing(TestRDFLoader):
    """
    Test the parsing of `RDFLoader` objects
    """

    def getLoader(self, graph):
        return skos.RDFLoader(graph, 1) # a parsing depth of 1

    def getExternalResource(self, resource):
        return 'file://' + os.path.join(os.path.dirname(os.path.abspath(__file__)), resource)
    
    def testSynonyms(self):
        concept = self.loader['http://portal.oceannet.org/test']
        key = self.getExternalResource('external1.xml')
        self.assertIn(key, concept.synonyms)
        match = concept.synonyms[key]
        self.assertIsInstance(match, skos.Concept)
        self.assertIn(concept, match.synonyms)

    def testRelated(self):
        concept = self.loader['http://portal.oceannet.org/test']
        self.assertEqual(len(concept.related), 2)
        keys = [self.getExternalResource('external2.xml'), 'http://portal.oceannet.org/test3']
        for key in keys:
            self.assertIn(key, concept.related)
            match = concept.related[key]
            self.assertIsInstance(match, skos.Concept)
            self.assertIn(concept, match.related)

    def testNarrower(self):
        concept = self.loader['http://portal.oceannet.org/test2']
        key = self.getExternalResource('external2.xml')
        self.assertIn(key, concept.narrower)
        match = concept.narrower[key]
        self.assertIsInstance(match, skos.Concept)
        self.assertIn(concept, match.broader)

    def testFlattening(self):
        self.loader.flat = True
        self.assertEqual(len(self.loader), 7)
        self.assertIn(self.getExternalResource('external2.xml'), self.loader)

class TestRDFUriNormalisation(TestRDFLoader):
    """
    Test the uri normalisation functionality
    """

    def getLoader(self, graph):
        def normalise_uri(uri):
            return uri.rstrip(u'/')
        return skos.RDFLoader(graph, normalise_uri=normalise_uri)

if __name__ == '__main__':
    unittest.main(verbosity=2)
