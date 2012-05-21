# -*- coding: utf-8 -*-

import skos
from test import unittest

class TestCase(unittest.TestCase):
    """
    Base class for testing `Concepts`
    """

    def getConcepts(self):
        """
        Return a list of concepts
        """
        return [
            skos.Concept('uri1', 'prefLabel1', 'definition1'),
            skos.Concept('uri2', 'prefLabel2', 'definition2'),
            skos.Concept('uri1', 'prefLabel1', 'definition1') # repeat a concept
            ]

class TestConceptsInit(TestCase):
    """
    Test `Concepts` initialisation
    """
    
    def testMapping(self):
        mapping = dict(((concept.uri, concept) for concept in self.getConcepts()))
        concepts = skos.Concepts(mapping)
        self.assertEqual(concepts._concepts, mapping)

    def testIterable(self):
        clist = self.getConcepts()
        concepts = skos.Concepts(clist)
        self.assertSequenceEqual(concepts._concepts.values(), list(set(clist)))

class TestConcepts(TestCase):
    """
    A base class used for testing `Concepts` objects
    """

    def setUp(self):
        self.concepts = skos.Concepts(self.getConcepts())

    def testLen(self):
        self.assertEqual(len(self.concepts), 2)

    def testIn(self):
        child = skos.Concept('uri1', 'prefLabel1', 'definition1')
        self.assertIn(child, self.concepts)
        self.assertIn(child.uri, self.concepts)

    def testIteration(self):
        original = set((concept.uri for concept in self.getConcepts()))
        new = set(iter(self.concepts))
        self.assertSetEqual(original, new)

    def testAdd(self):
        # add an existing child
        existing = skos.Concept('uri1', 'prefLabel1', 'definition1')
        self.concepts.add(existing)
        self.assertEqual(len(self.concepts), 2)

        # add a new child
        new = skos.Concept('uri3', 'prefLabel3', 'definition3')
        self.concepts.add(new)
        self.assertEqual(len(self.concepts), 3)
        
    def testDiscard(self):
        # discard an existing child
        existing = skos.Concept('uri1', 'prefLabel1', 'definition1')
        self.assertIn(existing, self.concepts)
        self.concepts.discard(existing)
        self.assertEqual(len(self.concepts), 1)
        self.assertNotIn(existing, self.concepts)

        # discard a non-extant concept
        external = skos.Concept('uri3', 'prefLabel3', 'definition3')
        self.assertNotIn(external, self.concepts)
        self.concepts.discard(external)
        self.assertEqual(len(self.concepts), 1)

    def testGetItem(self):
        value = self.concepts['uri1']
        self.assertIsInstance(value, skos.Concept)
        self.assertEqual(value.uri, 'uri1')

        with self.assertRaises(KeyError) as cm:
            self.concepts['uri3']

    def testDelItem(self):
        self.assertIn('uri2', self.concepts)
        del self.concepts['uri2']
        self.assertNotIn('uri2', self.concepts)

        with self.assertRaises(KeyError) as cm:
            del self.concepts['uri2']

    def testPop(self):
        value = self.concepts.pop()
        self.assertIsInstance(value, skos.Concept)
        
    def testRemove(self):
        child = self.concepts['uri2']
        self.concepts.remove(child)
        self.assertNotIn(child, self.concepts)

        with self.assertRaises(KeyError) as cm:
            self.concepts.remove(child)
        
    def testUpdateMapping(self):
        self.concepts.clear()
        mapping = dict(((concept.uri, concept) for concept in self.getConcepts()))
        self.concepts.update(mapping)
        self.assertEqual(self.concepts._concepts, mapping)

    def testUpdateIterable(self):
        self.concepts.clear()
        clist = self.getConcepts()
        self.concepts.update(clist)
        self.assertSequenceEqual(self.concepts._concepts.values(), list(set(clist)))

    def testEqual(self):
        other = skos.Concepts(self.getConcepts())
        self.assertEqual(self.concepts, other)
            
if __name__ == '__main__':
    unittest.main(verbosity=2)
