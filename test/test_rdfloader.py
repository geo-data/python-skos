# -*- coding: utf-8 -*-

import skos
from test import unittest
import rdflib
import os.path
import datetime

class TestRDFLoaderConstructor(unittest.TestCase):
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
    
    def __init__(self, rdf_files, *args, **kwargs):
        super(TestCase, self).__init__(*args, **kwargs)
        self.rdf_files = rdf_files

    def setUp(self):
        graph = rdflib.Graph()
        directory = os.path.dirname(__file__)
        for file_ in self.rdf_files:
            graph.parse(os.path.join(directory, file_))
        self.loader = self.getLoader(graph)

    def getLoader(self, graph):
        return skos.RDFLoader(graph, 0)

class TestUnicode(TestCase):

    def __init__(self, *args, **kwargs):
        rdf_files = [
            'concepts-unicode.xml',
            'schemes-unicode.xml'
        ]
        super(TestUnicode, self).__init__(rdf_files, *args, **kwargs)

    def testLen(self):
        self.assertEqual(len(self.loader), 3)

class TestRDFLoader(TestCase):
    """
    A base class used for testing `RDFLoader` objects
    """

    def __init__(self, *args, **kwargs):
        rdf_files = [
            'concepts-dce.xml',
            'schemes-dce.xml',
            'concepts-dc.xml',
            'schemes-dc.xml'
        ]
        super(TestRDFLoader, self).__init__(rdf_files, *args, **kwargs)

    def getKeys(self):
        return [
            'http://portal.oceannet.org/test',
            'http://portal.oceannet.org/test2',
            'http://portal.oceannet.org/test3',
            'http://portal.oceannet.org/collection',
            'http://example.com/thesaurus',
            'http://example.com/thesaurus/dc',
            'http://portal.oceannet.org/test/dc',
            'http://portal.oceannet.org/test2/dc',
            'http://portal.oceannet.org/collection/dc',
            'http://portal.oceannet.org/test3/dc'
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
        self.assertEqual(len(self.loader), 10)

    def testGetItem(self):
        value = self.loader['http://portal.oceannet.org/test']
        self.assertIsInstance(value, skos.Concept)

        value = self.loader['http://example.com/thesaurus']
        self.assertIsInstance(value, skos.ConceptScheme)

    def testGetConcepts(self):
        concepts = self.loader.getConcepts()
        self.assertIsInstance(concepts, skos.Concepts)
        self.assertEqual(len(concepts), 6)
        for concept in concepts.itervalues():
            self.assertIsInstance(concept, skos.Concept)
            self.assertGreater(len(concept.uri), 1)
            self.assertGreater(len(concept.prefLabel), 1)
            self.assertGreater(len(concept.definition), 1)
            self.assertGreater(len(concept.notation), 1)

    def testGetConceptSchemes(self):
        schemes = self.loader.getConceptSchemes()
        self.assertIsInstance(schemes, skos.Concepts)
        self.assertEqual(len(schemes), 2)
        for scheme in schemes.itervalues():
            self.assertIsInstance(scheme, skos.ConceptScheme)
            self.assertEqual(scheme.title, 'The SWAD-Europe Example Thesaurus')
            self.assertEqual(scheme.description, 'An example thesaurus to illustrate the use of the SKOS-Core schema.')

    def testGetCollections(self):
        collections = self.loader.getCollections()
        self.assertIsInstance(collections, skos.Concepts)
        self.assertEqual(len(collections), 2)
        for collection in collections.itervalues():
            self.assertIsInstance(collection, skos.Collection)
            self.assertEqual(collection.title, 'Test Collection')
            self.assertEqual(collection.description, 'A collection of concepts used as a test')
            self.assertIsInstance(collection.date, datetime.datetime)

    def testFlattening(self):
        self.loader.flat = True
        self.assertEqual(len(self.loader), 10)

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
        key = self.getExternalResource('external1-dce.xml')
        self.assertIn(key, concept.synonyms)
        match = concept.synonyms[key]
        self.assertIsInstance(match, skos.Concept)
        self.assertIn(concept, match.synonyms)

    def testRelated(self):
        concept = self.loader['http://portal.oceannet.org/test']
        self.assertEqual(len(concept.related), 2)
        keys = [self.getExternalResource('external2-dce.xml'), 'http://portal.oceannet.org/test3']
        for key in keys:
            self.assertIn(key, concept.related)
            match = concept.related[key]
            self.assertIsInstance(match, skos.Concept)
            self.assertIn(concept, match.related)

    def testNarrower(self):
        concept = self.loader['http://portal.oceannet.org/test2']
        key = self.getExternalResource('external2-dce.xml')
        self.assertIn(key, concept.narrower)
        match = concept.narrower[key]
        self.assertIsInstance(match, skos.Concept)
        self.assertIn(concept, match.broader)

    def testFlattening(self):
        self.loader.flat = True
        self.assertEqual(len(self.loader), 12)
        self.assertIn(self.getExternalResource('external2-dce.xml'), self.loader)

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
