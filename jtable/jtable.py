# -*- coding: utf-8 -*-
from sqlalchemy.sql.expression import asc, desc
from sqlalchemy.sql import or_, and_
from sqlalchemy.orm.properties import RelationshipProperty
from sqlalchemy.sql.expression import cast
from sqlalchemy import String

from collections import namedtuple

ColumnTuple = namedtuple('ColumnDT', ['column_name', 'mData', 'search_like', 'filter'])


def get_attr(sqla_object, attribute):
    """Returns the value of an attribute of an SQLAlchemy entity 
    """
    output = sqla_object
    for x in attribute.split('.'):
        output = getattr(output, x)
    return output


class ColumnDT(ColumnTuple):
    """Class defining a DataTables Column with a ColumnTuple:

    :param column_name: name of the column as defined by the SQLAlchemy model
    :type column_name: str
    :param mData: name of the mData property as defined in the DataTables javascript options (default None)
    :type mData: str
    :param search_like: search criteria to like on without forgetting to escape the '%' character
    :type search_like: str
    :param filter: the method needed to be executed on the cell values of the column 
    as an equivalent of a jinja2 filter (default None)
    :type filter: a callable object

    :returns: a ColumnDT object 
    """
    def __new__(cls, column_name, mData=None, search_like=None, filter=str):
        """
        On creation, sets default None values for mData and string value for
        filter (cause: Object representation is not JSON serializable)
        """
        return super(ColumnDT, cls).__new__(cls, column_name, mData, search_like, filter)


class JTable:
    """Class defining a JTables object with:

    :param request: request containing the GET values, specified by the 
    datatable for filtering, sorting and paging
    :type request: pyramid.request
    :param sqla_object: your SQLAlchemy table object
    :type sqla_object: sqlalchemy.ext.declarative.DeclarativeMeta
    :param query: the query wanted to be seen in the the table
    :type query: sqlalchemy.orm.query.Query
    :param columns: columns specification for the datatables
    :type columns: list

    :returns: a JTable object
    """
	def __init__(self, request, sqla_object, query, columns):
		"""Initializes the object with the attributes needed, and runs the query
		"""
		self.request_values = request.params
		self.sqla_object = sqla_object
		self.query = query
		self.columns = columns
		self.results = None
		
		self.paging_start = None
		self.paging_length = None
		
		# total in the table after filtering
		self.cardinality_filtered = 0

		# total in the table unfiltered
		self.cardinality = 0

		self.run()

	def output_result(self):
		"""Outputs the results in the format needed by DataTables
		"""
		output = {}
		output['TotalRecordCount'] = str(self.cardinality)
		output['Result'] = u'OK'

		output['Records'] = self.results

		return output

	def run(self):
		"""Launch filtering, sorting and paging processes to output results
		"""
		# count before filtering
		self.cardinality = self.query.count()

		# the term entered in the datatable's search box
		self.filtering()

		# field chosen to sort on
		self.sorting()

		# pages have a 'start' and 'length' attributes
		self.paging()

		# fetch the result of the queries
		self.results = self.query.all()

		#return formatted results with correct filters applied
		formatted_results = []
		for i in range(len(self.results)):
			row = dict()
			for j in range(len(self.columns)):
				col = self.columns[j]
				tmp_row = get_attr(self.results[i], col.column_name)
				if col.filter:
					tmp_row = col.filter(tmp_row)
				row[col.mData if col.mData else str(j)] = tmp_row
			formatted_results.append(row)

		self.results = formatted_results

	def filtering(self):
		"""Construct the query, by adding filtering(LIKE) on all columns when the datatable's search box is used
		"""
		search_value = self.request_values.get('sSearch')
		condition = None
		def search(idx, col):
			tmp_column_name = col.column_name.split('.')
			obj = getattr(self.sqla_object, tmp_column_name[0])
			if not hasattr(obj, "property"): # Ex: hybrid_property or property
				sqla_obj = self.sqla_object
				column_name = col.column_name
			elif isinstance(obj.property, RelationshipProperty): #Ex: ForeignKey
				# Ex: address.description
				sqla_obj = obj.mapper.class_
				column_name = "".join(tmp_column_name[1:])
				if not column_name:
					# find first primary key
					column_name = obj.property.table.primary_key.columns \
						.values()[0].name
			else:
				sqla_obj = self.sqla_object
				column_name = col.column_name
			return sqla_obj, column_name

		if search_value:
			conditions = []
			for idx, col in enumerate(self.columns):
				if self.request_values.get('bSearchable_%s' % idx) in (
						True, 'true'):
					sqla_obj, column_name = search(idx, col)
					conditions.append(cast(get_attr(sqla_obj, column_name), String).ilike('%%%s%%' % search_value))
			condition = or_(*conditions)
		conditions = []
		for idx, col in enumerate(self.columns):
			if self.request_values.get('sSearch_%s' % idx) in (True, 'true'):
				search_value2 = self.request_values.get('sSearch_%s' % idx)
				sqla_obj, column_name = search(idx, col)

				if col.search_like:
					conditions.append(cast(get_attr(sqla_obj, column_name), String).like(col.search_like % search_value2))
				else:
					conditions.append(cast(get_attr(sqla_obj, column_name), String).__eq__(search_value2))

				if condition is not None:
					condition = and_(condition, and_(*conditions))
				else:
					condition= and_(*conditions)

		if condition is not None:
			self.query = self.query.filter(condition)
			# count after filtering
			self.cardinality_filtered = self.query.count()
		else:
			self.cardinality_filtered = self.cardinality

	def sorting(self):
		"""Construct the query, by adding sorting(ORDER BY) on the columns needed to be applied on
		"""
		sorting = []

		Order = namedtuple('order', ['name', 'dir'])

		if self.request_values.has_key('jtSorting'):
			sort_terms = self.request_values['jtSorting'].split(',')
			for term in sort_terms:	
				term_parts = term.split()
				sorting.append(Order(term_parts[0], term_parts[1]))

		for sort in sorting:
			tmp_sort_name = sort.name.split('.')
			obj = getattr(self.sqla_object, tmp_sort_name[0])
			if not hasattr(obj, "property"): #hybrid_property or property
				sort_name = sort.name

				if hasattr(self.sqla_object, "__tablename__"):
					tablename = self.sqla_object.__tablename__
				else:
					tablename = self.sqla_object.__table__.name
			elif isinstance(obj.property, RelationshipProperty): # Ex: ForeignKey
				 # Ex: address.description => description => addresses.description
				sort_name = "".join(tmp_sort_name[1:])
				if not sort_name:
					# Find first primary key
					sort_name = obj.property.table.primary_key.columns \
							.values()[0].name
				tablename = obj.property.table.name
			else: #-> ColumnProperty
				sort_name = sort.name

				if hasattr(self.sqla_object, "__tablename__"):
					tablename = self.sqla_object.__tablename__
				else:
					tablename = self.sqla_object.__table__.name

			sort_name = "%s.%s" % (tablename, sort_name)
			self.query = self.query.order_by(
				asc(sort_name) if sort.dir == 'asc' else desc(sort_name))

	def paging(self):
		"""Construct the query, by slicing the results in order to limit rows showed on the page, and paginate the rest
		"""
		if self.request_values.has_key('jtStartIndex'):
			self.paging_start = int(self.request_values['jtStartIndex'])
		if self.request_values.has_key('jtPageSize'):
			self.paging_length = int(self.request_values['jtPageSize'])
		if (self.paging_start and self.paging_length):
			self.query = self.query.slice(pages.start, offset)
