# See the SKOS Primer <http://www.w3.org/TR/skos-primer> for an
# introduction to SKOS

from sqlalchemy.ext.declarative import declarative_base
#from sqlalchemy import Table, Column, Integer, String, Date, Float, ForeignKey, event
from sqlalchemy import Table, Column, String, Text, ForeignKey
from sqlalchemy.orm import relationship, backref, synonym
from sqlalchemy.orm.collections import attribute_mapped_collection, collection
import collections
import logging

logger = logging.getLogger(__name__)

def info(*args, **kwargs):
    logger.info(*args, **kwargs)

def debug(*args, **kwargs):
    logger.debug(*args, **kwargs)

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

class RecursionError(Exception):
    pass

# This class is necessary as the first option described at
# <http://groups.google.com/group/sqlalchemy/browse_thread/thread/b4eaef1bdf132cdc?pli=1>
# for a solution to self-referential many-to-many relationships using
# the same property does not seem to be writable.
class ExactMatches(collections.MutableSet, collections.Mapping):
    """
    Provides an interface to `Concept` exact matches

    This is returned by `Concept.exactMatches`.
    """

    def __init__(self, concept):
        self._concept = concept

    # Implement the interface for `collections.Iterable`
    def __iter__(self):
        self._concept._exactMatches_left.update(self._concept._exactMatches_right)
        return iter(self._concept._exactMatches_left)

    # Implement the interface for `collections.Container`
    def __contains__(self, value):
        return value in self._concept._exactMatches_left or value in self._concept._exactMatches_right

    # Implement the interface for `collections.Sized`
    def __len__(self):
        return len(set(self._concept._exactMatches_left.keys() + self._concept._exactMatches_right.keys()))

    # Implement the interface for `collections.MutableSet`
    def add(self, value):
        self._concept._exactMatches_left[value.uri] = value

    def discard(self, value):
        try:
            del self._concept._exactMatches_left[value.uri]
        except KeyError:
            pass
        try:
            del self._concept._exactMatches_right[value.uri]
        except KeyError:
            pass

    # Implement the interface for `collections.Mapping` with the
    # ability to delete items as well

    def __getitem__(self, key):
        try:
            return self._concept._exactMatches_left[key]
        except KeyError:
            pass

        try:
            return self._concept._exactMatches_right[key]
        except KeyError, e:
            raise e

    def __delitem__(self, key):
        deleted = False
        try:
            del self._concept._exactMatches_left[key]
        except KeyError:
            pass
        else:
            deleted = True

        try:
            del self._concept._exactMatches_right[key]
        except KeyError, e:
            if not deleted:
                raise e

    def __repr__(self):
        return repr(dict(self))

    def __str__(self):
        return str(dict(self))

# see <http://docs.sqlalchemy.org/en/latest/orm/collections.html> for
# details on custom collections.
class Concepts(collections.MutableSet, collections.Mapping):
    """
    A collection of Concepts

    This is a composition of the `collections.MutableSet` and
    `collections.Mapping` classes. It is *not* a `skos:Collection`
    implementation.
    """

    def __init__(self, values=None):
        self._concepts = {}
        if values:
            self.update(values)

    # Implement the interface for `collections.Iterable`
    def __iter__(self):
        return iter(self._concepts)

    @collection.iterator
    def itervalues(self):
        return super(Concepts, self).itervalues()

    # Implement the interface for `collections.Container`
    def __contains__(self, value):
        return value in self._concepts

    # Implement the interface for `collections.Sized`
    def __len__(self):
        return len(self._concepts)

    # Implement the interface for `collections.MutableSet`
    @collection.appender
    def add(self, value):
        self._concepts[value.uri] = value

    @collection.remover
    def discard(self, value):
        try:
            del self._concepts[value.uri]
        except KeyError:
            pass

    # Implement the interface for `collections.Mapping` with the
    # ability to delete items as well

    def __getitem__(self, key):
        return self._concepts[key]

    def __delitem__(self, key):
        self.discard(self._concepts[key]) # remove through an instrumented method

    @collection.converter
    @collection.internally_instrumented
    def update(self, concepts):
        """
        Update the concepts from another source

        The argument can be a dictionary-like container of concepts or
        a sequence of concepts.
        """
        debug('adding concepts %s', concepts)
        if not isinstance(concepts, collections.Mapping):
            return iter(concepts)
        return concepts.itervalues()

    def __eq__(self, other):
        return self._concepts == other

    def __str__(self):
        return str(self._concepts)

    def __repr__(self):
        return repr(self._concepts)

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
        collection_class=Concepts,
        backref=backref('narrowMatches', collection_class=Concepts))

    # many to many Concept <-> Concept representing exact matches
    _exactMatches_left = relationship(
        'Concept',
        secondary=concept_exact,
        primaryjoin=uri==concept_exact.c.left_uri,
        secondaryjoin=uri==concept_exact.c.right_uri,
        collection_class=attribute_mapped_collection('uri'),
        backref=backref('_exactMatches_right', collection_class=attribute_mapped_collection('uri')))

    def _getExactMatches(self):
        return ExactMatches(self)

    def _setExactMatches(self, values):
        self._exactMatches_left = values
        self._exactMatches_right = {}

    exactMatches = synonym('_exactMatches_left', descriptor=property(_getExactMatches, _setExactMatches))

    def __repr__(self):
        return "<%s('%s', '%s')>" % (self.__class__.__name__, self.uri, self.prefLabel)

    def __hash__(self):
        return hash(''.join((str(getattr(self, attr)) for attr in ('uri', 'prefLabel', 'definition'))))


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
    description = Column(Text, nullable=True)

    def __init__(self, uri, title, description=None):
        self.uri = uri
        self.title = title
        self.description = description

    concepts = relationship(
        'Concept',
        secondary=concepts2schemes,
        collection_class=Concepts,
        backref=backref('schemes', collection_class=Concepts))

    def __repr__(self):
        return "<%s('%s')>" % (self.__class__.__name__, self.uri)

    def __hash__(self):
        return hash(''.join((str(getattr(self, attr)) for attr in ('uri', 'title', 'description'))))


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
        return self.loadConceptSchemes(graph).union(self.loadConcepts(graph))

    def loadConceptSchemes(self, graph):
        self.cache = {}

        # get the skos:ConceptScheme
        for subject in graph.subjects(
            object=rdflib.URIRef('http://www.w3.org/2004/02/skos/core#ConceptScheme'),
            predicate=rdflib.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type')):
            self.loadConceptScheme(graph, subject)

        values = set(self.cache.itervalues())
        self.cache = {}
        return values

    def loadConcepts(self, graph):
        self.cache = {}

        # get the skos:Concept subjects
        for subject in graph.subjects(
            object=rdflib.URIRef('http://www.w3.org/2004/02/skos/core#Concept'),
            predicate=rdflib.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type')):
            self.loadConcept(graph, subject)

        values = set(self.cache.itervalues())
        self.cache = {}
        return values

    def _getItem(self, graph, subject, subject_type, depth=0):
        # try and return a cached item
        uri = str(subject)
        try:
            return self.cache[uri]
        except KeyError:
            pass

        if depth > self.max_depth:
            info('parsing depth (%d) exceeded for %s', depth, uri)
            raise RecursionError('parsing depth (%d) exceeded for %s' % (depth, uri))

        if (
            subject,
            rdflib.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
            rdflib.URIRef('http://www.w3.org/2004/02/skos/core#%s' % subject_type)
            ) not in graph:
            # try and parse the subject to add it to the current graph
            info('parsing %s %s', subject_type, uri)
            subgraph = graph.parse(uri)
            if (
            subject,
            rdflib.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
            rdflib.URIRef('http://www.w3.org/2004/02/skos/core#%s' % subject_type)
            ) not in subgraph:
                # the newly parsed graph didn't contain the concept
                debug('%s does not exist: %s', subject_type, uri)
                raise KeyError('%s does not exist: %s' % (subject_type, uri))

        debug('%s exists: %s', subject_type, uri)
        return None

    def loadConceptScheme(self, graph, subject):
        try:
            scheme = self._getItem(graph, subject, 'ConceptScheme')
        except (RecursionError, KeyError):
            return None

        if scheme:
            return scheme

        # create the basic concept
        uri = str(subject)
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
        try:
            concept = self._getItem(graph, subject, 'Concept', depth)
        except (RecursionError, KeyError):
            return None

        if concept:
            return concept
        depth += 1

        # create the basic concept
        uri = str(subject)
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
