# See the SKOS Primer <http://www.w3.org/TR/skos-primer> for an
# introduction to SKOS

from sqlalchemy.ext.declarative import declarative_base
#from sqlalchemy import Table, Column, Integer, String, Date, Float, ForeignKey, event
from sqlalchemy import Table, Column, String, Text, ForeignKey
from sqlalchemy.orm import relationship, backref, synonym
import collections

# Create a SQLAlchemy declarative base class using our metaclass
Base = declarative_base()

# association tables for many to many joins
concept_broader = Table('concept_broader', Base.metadata,
    Column('broader_uri', String(255), ForeignKey('concept.uri')),
    Column('narrower_uri', String(255), ForeignKey('concept.uri'))
)

concept_exact = Table('concept_exact', Base.metadata,
    Column('left_uri', String(255), ForeignKey('concept.uri')),
    Column('right_uri', String(255), ForeignKey('concept.uri'))
)

concepts2schemes = Table('concepts2schemes', Base.metadata,
    Column('scheme_uri', String(255), ForeignKey('concept_scheme.uri')),
    Column('concept_uri', String(255), ForeignKey('concept.uri'))
)

# This class is necessary as the first option described at
# <http://groups.google.com/group/sqlalchemy/browse_thread/thread/b4eaef1bdf132cdc?pli=1>
# for a solution to self-referential many-to-many relationships using
# the same property does not seem to be writable.
class ExactMatches(collections.Set):
    """
    Provides an interface to a concepts exact matches
    """

    def __init__(self, concept):
        self._concept = concept

    def __iter__(self):
        for item in self._concept._exactMatches_left:
            yield item

        for item in self._concept._exactMatches_right:
            yield item

    def __contains__(self, value):
        return value in self._concept._exactMatches_left or value in self._concept._exactMatches_right

    def __len__(self):
        return len(self._concept._exactMatches_left.union(self._concept._exactMatches_right))

    def add(self, value):
        return self._concept._exactMatches_left.add(value)

    def discard(self, value):
        self._concept._exactMatches_left.discard(value)
        self._concept._exactMatches_right.discard(value)

    def __repr__(self):
        return repr(self._concept._exactMatches_left.union(self._concept._exactMatches_right))

    def __str__(self):
        return str(self._concept._exactMatches_left.union(self._concept._exactMatches_right))

class Concept(Base):
    __tablename__ = 'concept'

    uri = Column(String(255), primary_key=True, nullable=False)
    prefLabel = Column(String(50), nullable=False)
    definition = Column(Text, nullable=False)

    def __init__(self, uri, prefLabel, definition):
        self.uri = uri
        self.prefLabel = prefLabel
        self.definition = definition

    # many to many Concept <-> Concept representing broadness <->
    # narrowness
    broadMatches = relationship(
        'Concept',
        secondary=concept_broader,
        primaryjoin=uri==concept_broader.c.narrower_uri,
        secondaryjoin=uri==concept_broader.c.broader_uri,
        collection_class=set,
        backref=backref('narrowMatches', collection_class=set))

    # many to many Concept <-> Concept representing exact matches
    _exactMatches_left = relationship(
        'Concept',
        secondary=concept_exact,
        primaryjoin=uri==concept_exact.c.left_uri,
        secondaryjoin=uri==concept_exact.c.right_uri,
        collection_class=set,
        backref=backref('_exactMatches_right', collection_class=set))

    def _getExactMatches(self):
        return ExactMatches(self)

    def _setExactMatches(self, values):
        values = set(values)
        self._exactMatches_left = values
        self._exactMatches_right = values

    exactMatches = synonym('_exactMatches_left', descriptor=property(_getExactMatches, _setExactMatches))

    def __repr__(self):
        return "<%s('%s', '%s')>" % (self.__class__.__name__, self.uri, self.prefLabel)

class ConceptScheme(Base):
    """
    Represents a set of Concepts

    `skos:ConceptScheme` is a set of concepts, optionally including
    statements about semantic relationships between those
    concepts. Thesauri, classification schemes, subject-heading lists,
    taxonomies, terminologies, glossaries and other types of
    controlled vocabulary are all examples of concept schemes
    """

    __tablename__ = 'concept_scheme'

    uri = Column(String(255), primary_key=True, nullable=False)
    title = Column(String(255), nullable=False)
    description = Column(Text, nullable=False)

    def __init__(self, uri, title, description):
        self.uri = uri
        self.title = title
        self.description = description

    members = relationship(
        'Concept',
        secondary=concepts2schemes,
        collection_class=set,
        backref=backref('schemes', collection_class=set))

    def __repr__(self):
        return "<%s('%s', '%s')>" % (self.__class__.__name__, self.uri, self.title)

import rdflib
class RDFLoader(object):
    """
    Turn a RDF graph in to Concept classes
    """

    max_depth = None
    def __init__(self, max_depth=float('inf')):
        self.cache = {}
        self.max_depth = max_depth

    def load(self, graph):
        return self.loadConceptSchemes(graph) + self.loadConcepts(graph)

    def loadConceptSchemes(self, graph):
        self.cache = {}

        # get the skos:ConceptScheme
        for subject in graph.subjects(
            object=rdflib.URIRef('http://www.w3.org/2004/02/skos/core#ConceptScheme'),
            predicate=rdflib.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type')):
            self.loadConceptScheme(graph, subject)        

        values = self.cache.values()
        self.cache = {}
        return values

    def loadConcepts(self, graph):
        self.cache = {}

        # get the skos:Concept subjects
        for subject in graph.subjects(
            object=rdflib.URIRef('http://www.w3.org/2004/02/skos/core#Concept'),
            predicate=rdflib.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type')):
            self.loadConcept(graph, subject)

        values = self.cache.values()
        self.cache = {}
        return values

    def loadConceptScheme(self, graph, subject, depth=0):

        # try and return a cached concept
        uri = str(subject)
        try:
            return self.cache[uri]
        except KeyError:
            pass

        if depth > self.max_depth:
            print 'parsing depth (%d) exceeded for %s' % (depth, uri)
            return None
        depth += 1

        if (
            subject,
            rdflib.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
            rdflib.URIRef('http://www.w3.org/2004/02/skos/core#ConceptScheme')
            ) not in graph:
            # try and parse the subject to add it to the current graph
            print 'parsing', subject
            subgraph = graph.parse(str(subject))
            if (
            subject,
            rdflib.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
            rdflib.URIRef('http://www.w3.org/2004/02/skos/core#ConceptScheme')
            ) not in subgraph:
                return None     # the newly parsed graph didn't contain the concept
        else:
            print subject, 'is present'

        # create the basic concept
        try:
            title = list(graph.objects(subject=subject, predicate=rdflib.URIRef('http://purl.org/dc/elements/1.1/title')))[0]
        except IndexError:
            raise ValueError('Expected a title for the ConceptScheme: %s' % uri)

        try:
            description = list(graph.objects(subject=subject, predicate=rdflib.URIRef('http://purl.org/dc/elements/1.1/description')))[0]
        except IndexError:
            raise ValueError('Expected a description for the ConceptScheme: %s' % uri)

        scheme = ConceptScheme(uri, str(title), str(description))
        self.cache[uri] = scheme
        return scheme

    def loadConcept(self, graph, subject, depth=0):
        # try and return a cached concept
        uri = str(subject)
        try:
            return self.cache[uri]
        except KeyError:
            pass
        
        if depth > self.max_depth:
            print 'parsing depth (%d) exceeded for %s' % (depth, uri)
            return None
        depth += 1

        if (
            subject,
            rdflib.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
            rdflib.URIRef('http://www.w3.org/2004/02/skos/core#Concept')
            ) not in graph:
            # try and parse the subject to add it to the current graph
            print 'parsing', subject
            subgraph = graph.parse(str(subject))
            if (
            subject,
            rdflib.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
            rdflib.URIRef('http://www.w3.org/2004/02/skos/core#Concept')
            ) not in subgraph:
                return None     # the newly parsed graph didn't contain the concept
        else:
            print subject, 'is present'


        # create the basic concept
        prefLabel = list(graph.objects(subject=subject, predicate=rdflib.URIRef('http://www.w3.org/2004/02/skos/core#prefLabel')))[0]
        definition = list(graph.objects(subject=subject, predicate=rdflib.URIRef('http://www.w3.org/2004/02/skos/core#definition')))[0]
        concept = Concept(uri, str(prefLabel), str(definition))
        self.cache[uri] = concept

        # add any narrow matches
        for obj in graph.objects(subject=subject, predicate=rdflib.URIRef('http://www.w3.org/2004/02/skos/core#narrowMatch')):
            narrowMatch = self.loadConcept(graph, obj, depth)
            if narrowMatch:
                concept.narrowMatches.add(narrowMatch)

        # add any broad matches
        for obj in graph.objects(subject=subject, predicate=rdflib.URIRef('http://www.w3.org/2004/02/skos/core#broadMatch')):
            broadMatch = self.loadConcept(graph, obj, depth)
            if broadMatch:
                concept.broadMatches.add(broadMatch)

        # add any exact matches
        for obj in graph.objects(subject=subject, predicate=rdflib.URIRef('http://www.w3.org/2004/02/skos/core#exactMatch')):
            exactMatch = self.loadConcept(graph, obj, depth)
            if exactMatch:
                concept.exactMatches.add(exactMatch)

        return concept
