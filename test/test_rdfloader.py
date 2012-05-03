# -*- coding: utf-8 -*-

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import skos
import unittest
import rdflib
import os.path

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
        self.assertEqual(len(self.loader), 3)

    def testGetItem(self):
        value = self.loader['http://portal.oceannet.org/test']
        self.assertIsInstance(value, skos.Concept)

        value = self.loader['http:/example.com/thesaurus']
        self.assertIsInstance(value, skos.ConceptScheme)

    def testGetConcepts(self):
        concepts = self.loader.getConcepts()
        self.assertIsInstance(concepts, skos.Concepts)
        self.assertEqual(len(concepts), 2)
        for concept in concepts.itervalues():
            self.assertIsInstance(concept, skos.Concept)

    def testGetConceptSchemes(self):
        schemes = self.loader.getConceptSchemes()
        self.assertIsInstance(schemes, skos.Concepts)
        self.assertEqual(len(schemes), 1)
        for scheme in schemes.itervalues():
            self.assertIsInstance(scheme, skos.ConceptScheme)

class TestRDFParsing(TestRDFLoader):
    """
    Test the parsing of `RDFLoader` objects
    """

    def getLoader(self, graph):
        return skos.RDFLoader(graph, 1) # a parsing depth of 1

    def getExternalResource(self, resource):
        return 'file://' + os.path.join(os.path.dirname(os.path.abspath(__file__)), resource)
    
    def testExactMatches(self):
        concept = self.loader['http://portal.oceannet.org/test']
        key = self.getExternalResource('external1.xml')
        self.assertIn(key, concept.exactMatches)
        match = concept.exactMatches[key]
        self.assertIsInstance(match, skos.Concept)
        self.assertIn(concept, match.exactMatches)

    def testNarrowMatches(self):
        concept = self.loader['http://portal.oceannet.org/test2']
        key = self.getExternalResource('external2.xml')
        self.assertIn(key, concept.narrowMatches)
        match = concept.narrowMatches[key]
        self.assertIsInstance(match, skos.Concept)
        self.assertIn(concept, match.broadMatches)

    def testFlattening(self):
        self.loader.flat = True
        self.assertEqual(len(self.loader), 5)
        self.assertIn(self.getExternalResource('external2.xml'), self.loader)

if __name__ == '__main__':
    unittest.main(verbosity=2)
