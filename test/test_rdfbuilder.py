# -*- coding: utf-8 -*-

import skos
from test import unittest
from datetime import datetime
from iso8601.iso8601 import UTC
import rdflib

class TestCase(unittest.TestCase):

    def getConcepts(self):
        concepts = [
            skos.Concept('uri1', 'prefLabel1', 'definition1', 'notation1', 'altLabel1'),
            skos.Concept('uri2', 'prefLabel2', 'definition2', 'notation2', 'altLabel2')
            ]
        return concepts

    def getCollection(self):
        collection = skos.Collection('uri', 'title', 'description', datetime(2012, 5, 24, 20, 35, 34, 489923, UTC))
        collection.members = self.getConcepts()
        return collection
    
    def setUp(self):
        self.builder = skos.RDFBuilder()

    def testBuild(self):
        objects = self.getConcepts()
        collection = self.getCollection()
        objects.append(collection)
        graph = self.builder.build(objects)
        self.assertEqual(len(graph), 16)

        # round trip the data
        loader = skos.RDFLoader(graph)
        self.assertEqual(len(loader), 3)

        for obj in objects:
            self.assertIn(obj.uri, loader)
            self.assertEqual(loader[obj.uri], obj)

if __name__ == '__main__':
    unittest.main(verbosity=2)
