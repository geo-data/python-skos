# See the SKOS Primer <http://www.w3.org/TR/skos-primer> for an
# introduction to SKOS

from sqlalchemy.ext.declarative import declarative_base
#from sqlalchemy import Table, Column, Integer, String, Date, Float, ForeignKey, event
from sqlalchemy import Table, Column, String, Text, ForeignKey
from sqlalchemy.orm import relationship, backref, synonym
from sqlalchemy.orm.collections import collection
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
        self._concept._exactMatches_left.add(value)

    def discard(self, value):
        self._concept._exactMatches_left.discard(value)
        self._concept._exactMatches_right.discard(value)

    def pop(self):
        try:
            value = self._concepts._exactMatches_left.pop()
        except KeyError:
            value = self._concepts._exactMatches_right.pop()
            self._concepts._exactMatches_left.discard(value)
        else:
            self._concepts._exactMatches_right.discard(value)
        return value

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

    def __eq__(self, other):
        return (
            self._concept._exactMatches_right == other._concept._exactMatches_right and
            self._concept._exactMatches_left == other._concept._exactMatches_left
            )

class Concepts(collections.Mapping, collections.MutableSet):
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

    # Implement the interface for `collections.Container`
    def __contains__(self, value):
        if isinstance(value, Concept):
            value = value.uri
        return value in self._concepts

    # Implement the interface for `collections.Sized`
    def __len__(self):
        return len(self._concepts)

    # Implement the interface for `collections.MutableSet`
    def add(self, value):
        self._concepts[value.uri] = value

    def discard(self, value):
        try:
            del self._concepts[value.uri]
        except KeyError:
            pass

    def pop(self):
        key, value = self._concepts.popitem()
        return value

    # Implement the interface for `collections.Mapping` with the
    # ability to delete items as well

    def __getitem__(self, key):
        return self._concepts[key]

    def __delitem__(self, key):
        self.discard(self._concepts[key]) # remove through an instrumented method

    def update(self, concepts):
        """
        Update the concepts from another source

        The argument can be a dictionary-like container of concepts or
        a sequence of concepts.
        """
        if not isinstance(concepts, collections.Mapping):
            iterator = iter(concepts)
        else:
            iterator = concepts.itervalues()
        for value in iterator:
            self.add(value)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._concepts == other._concepts
        return self._concepts == other

    def __str__(self):
        return str(self._concepts)

    def __repr__(self):
        return repr(self._concepts)

class InstrumentedConcepts(Concepts):
    """
    Adapted `Concepts` class for use in SQLAlchemy relationships
    """

    # See <http://docs.sqlalchemy.org/en/latest/orm/collections.html>
    # for details on custom collections. This also uses the "trivial
    # subclass" trick detailed at
    # <http://docs.sqlalchemy.org/en/latest/orm/collections.html#instrumentation-and-custom-types>.

    @collection.iterator
    def itervalues(self):
        return super(InstrumentedConcepts, self).itervalues()

    @collection.appender
    def add(self, *args, **kwargs):
        return super(InstrumentedConcepts, self).add(*args, **kwargs)

    @collection.remover
    def discard(self, *args, **kwargs):
        return super(InstrumentedConcepts, self).discard(*args, **kwargs)

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
        collection_class=InstrumentedConcepts,
        backref=backref('narrowMatches', collection_class=InstrumentedConcepts))

    # many to many Concept <-> Concept representing exact matches
    _exactMatches_left = relationship(
        'Concept',
        secondary=concept_exact,
        primaryjoin=uri==concept_exact.c.left_uri,
        secondaryjoin=uri==concept_exact.c.right_uri,
        collection_class=InstrumentedConcepts,
        backref=backref('_exactMatches_right', collection_class=InstrumentedConcepts))

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

    def __eq__(self, other):
        return min([getattr(self, attr) == getattr(other, attr) for attr in ('uri', 'prefLabel', 'definition')])

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
        collection_class=InstrumentedConcepts,
        backref=backref('schemes', collection_class=InstrumentedConcepts))

    def __repr__(self):
        return "<%s('%s')>" % (self.__class__.__name__, self.uri)

    def __hash__(self):
        return hash(''.join((str(getattr(self, attr)) for attr in ('uri', 'title', 'description'))))

    def __eq__(self, other):
        return min([getattr(self, attr) == getattr(other, attr) for attr in ('uri', 'title', 'description', 'concepts')])


import rdflib
from itertools import chain
class RDFLoader(collections.Mapping):
    """
    Turn a RDF graph in to SKOS classes
    """

    graph = None
    max_depth = None
    def __init__(self, graph, max_depth=float('inf')):
        self.graph = graph
        self._cache = {}
        self.max_depth = max_depth
        self._parsing_depth = 0
        self.types = ('Concept', 'ConceptScheme')

    def _iterateTypes(self, types=None):
        if not types:
            types = self.types

        for subject_type in types:
            if subject_type not in self.types:
                raise ValueError('Bad SKOS type: %s' % subject_type)

            for subject in self.graph.subjects(
                object=rdflib.URIRef('http://www.w3.org/2004/02/skos/core#%s' % subject_type),
                predicate=rdflib.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type')):
                yield str(subject)

    # Implement the interface for `collections.Iterable`
    def __iter__(self):
        return self._iterateTypes()

    # Implement the interface for `collections.Container`
    def __contains__(self, value):
        subject = rdflib.URIRef(value)
        return max([(
            subject,
            rdflib.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
            rdflib.URIRef('http://www.w3.org/2004/02/skos/core#%s' % subject_type)
            ) in self.graph for subject_type in self.types])

    # Implement the interface for `collections.Sized`
    def __len__(self):
        return sum((1 for k in self))

    # Implement the interface for `collections.Mapping`
    def __getitem__(self, key):
        # try and return a cached item
        try:
            return self._cache[key]
        except KeyError, e:
            pass

        subject = rdflib.URIRef(key)
        predicate = rdflib.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type')
        for subject_type in self.types:
            try:
                loader = getattr(self, 'load' + subject_type)
            except AttributeError:
                raise ValueError('Bad SKOS type: %s' % subject_type)

            object_ = rdflib.URIRef('http://www.w3.org/2004/02/skos/core#%s' % subject_type)

            for triple in self.graph.triples((
                    subject,
                    predicate,
                    object_
                    )):
                try:
                    return loader(subject) # return the first value we come across
                finally:
                    if self._parsing_depth > 0:
                        self._parsing_depth -= 1

        raise KeyError(key)

    def _checkItem(self, subject, subject_type, graph=None):
        if not graph:
            graph = self.graph
        uri = str(subject)

        if (
            subject,
            rdflib.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
            rdflib.URIRef('http://www.w3.org/2004/02/skos/core#%s' % subject_type)
            ) not in self.graph:
            # try and parse the subject to add it to the current graph
            if self._parsing_depth >= self.max_depth:
                info('parsing depth (%d) exceeded for %s', self._parsing_depth, uri)
                raise RecursionError('parsing depth (%d) exceeded for %s' % (self._parsing_depth, uri))

            info('parsing %s %s', subject_type, uri)
            # parse using a new graph object. We need a new graph
            # because otherwise the existing graph is extended by
            # parse which dynamically affects the content of the
            # current RDFLoader
            graph = rdflib.Graph()
            graph.parse(uri)
            self._parsing_depth += 1
            if (
            subject,
            rdflib.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
            rdflib.URIRef('http://www.w3.org/2004/02/skos/core#%s' % subject_type)
            ) not in graph:
                # the newly parsed graph didn't contain the concept
                debug('%s does not exist: %s', subject_type, uri)
                raise KeyError('%s does not exist: %s' % (subject_type, uri))

        debug('%s exists: %s', subject_type, uri)
        return graph

    def loadConceptScheme(self, subject):
        self._checkItem(subject, 'ConceptScheme')

        # create the basic concept
        uri = str(subject)
        try:
            title = list(self.graph.objects(subject=subject, predicate=rdflib.URIRef('http://purl.org/dc/elements/1.1/title')))[0]
        except IndexError:
            raise ValueError('Expected a title for the ConceptScheme: %s' % uri)

        try:
            description = list(self.graph.objects(subject=subject, predicate=rdflib.URIRef('http://purl.org/dc/elements/1.1/description')))[0]
        except IndexError:
            raise ValueError('Expected a description for the ConceptScheme: %s' % uri)

        scheme = self._cache[uri] = ConceptScheme(uri, str(title), str(description))
        return scheme

    def loadConcept(self, subject, graph=None):
        uri = str(subject)

        # try and return a cached item
        try:
            return self._cache[uri]
        except KeyError:
            pass

        graph = self._checkItem(subject, 'Concept', graph)

        # create the basic concept
        prefLabel = list(graph.objects(subject=subject, predicate=rdflib.URIRef('http://www.w3.org/2004/02/skos/core#prefLabel')))[0]
        definition = list(graph.objects(subject=subject, predicate=rdflib.URIRef('http://www.w3.org/2004/02/skos/core#definition')))[0]
        concept = Concept(uri, str(prefLabel), str(definition))
        self._cache[uri] = concept

        def add_matches(concept, attr):
            attr_name = attr + 'es' # pluralise the attribute
            for obj in graph.objects(subject=subject, predicate=rdflib.URIRef('http://www.w3.org/2004/02/skos/core#%s' % attr)):
                key = str(obj)
                try:
                    match = self.loadConcept(obj, graph)
                except RecursionError:
                    continue
                if match:
                    getattr(concept, attr_name).add(match)

        add_matches(concept, 'narrowMatch') # add any narrow matches
        add_matches(concept, 'broadMatch') # add any broad matches
        add_matches(concept, 'exactMatch') # add any exact matches

        return concept

    def getConcepts(self):
        return Concepts([self[key] for key in self._iterateTypes(['Concept'])])

    def getConceptSchemes(self):
        return Concepts([self[key] for key in self._iterateTypes(['ConceptScheme'])])
