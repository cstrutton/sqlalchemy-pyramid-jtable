========================================
sqlalchemy-pyramid-jtable |Build Status|
========================================

.. |Build Status| image:: https://travis-ci.org/cstrutton/sqlalchemy-pyramid-jtable.png?branch=master
    :target: https://travis-ci.org/cstrutton/sqlalchemy-pyramid-jtable


Usage
-----

The package is soon to be available on PyPI

.. code-block:: bash

    pip install sqlalchemy-datatables


A simple example
----------------

**models.py**

.. code-block:: python

    class User(Base): 
        __tablename__ = 'users' 
        
        id          = Column(Integer, primary_key=True)::
        name        = Column(Text, unique=True)
        created_at  = Column(DateTime, default=datetime.datetime.utcnow)

        address     = relationship("Address", uselist=False, backref="user")

        def __init__(self, name):
            self.name = name

    class Address(Base):
        __tablename__ = 'addresses'

        id          = Column(Integer, primary_key=True)
        description = Column(Text, unique=True)
        user_id     = Column(Integer, ForeignKey('users.id'))

        def __init__(self, description):
            self.description = description

**views.py**

.. code-block:: python

    from .models import DBSession, User
    from datatables import ColumnDT, DataTables

    def _upper(chain):
        ret = chain.upper()
        if ret:
            return ret
        else:
            return chain

    @view_config(route_name='home', renderer='templates/home.pt')
    def home(request):
        return {'project': 'test-project'}

    @view_config(route_name='simple_example', request_method='GET', renderer='json')
    def simple_example(request):
        # defining columns
        columns = []
        columns.append(ColumnDT('id'))
        columns.append(ColumnDT('name', filter=_upper))
        columns.append(ColumnDT('address.description')) # where address is an SQLAlchemy Relation
        columns.append(ColumnDT('created_at', filter=str))

        # defining the initial query depending on your purpose
        query = DBSession.query(User).join(Address).filter(Address.id > 14)

        # instantiating a DataTable for the query and table needed
        rowTable = DataTables(request, User, query, columns) 

        # returns what is needed by DataTable 
        return rowTable.output_result()


**templates/home.pt**

.. code-block:: html

    <!--do your stuff-->
    <table class="table" id="simple-example">
        <thead>
            <tr>
                <th>Id</th>
                <th>User name</th>
                <th>Address description</th>
                <th>Created at</th>
            </tr>
        </thead>
        <tbody></tbody>
    </table>
    <!--do your stuff-->
    <script type="text/javascript" charset="utf-8">
        $(document).ready(function(){
            $('#simple-example').dataTable({
                "bProcessing": true,
                "bServerSide": true,
                "sAjaxSource": "${request.route_path('simple_example')}"
            });
        });
    </script>


Testing the Pyramid based ''test-project''
------------------------------------------

.. code-block:: bash

    $ git clone 
    $ virtualenv --no-site-packages sqlalchemy-datatables
    $ cd sqlalchemy-datatables/
    Once only: 
    $ bin/python setup.py develop

    $ cd test-project/
    $ $venv/bin/python setup.py develop
    $ $venv/bin/initialize_test-project_db development.ini
    $ $venv/bin/pserve development.ini

    Open a web browser and got to the url localhost:6543


Changelog
---------

**v0.1.1 (12/08/2013)**

-  Udate readme and change log to reflect progress.
-  Initial updates to jtable.py (proof of concept)
-  TODO:  need to update sample project verify all functionality

**v0.1.0 (6/12/2013)**

-  First version
-  Forked from `Pegase745/sqlalchemy-datatables <https://github.com/pegase745/sqlalchemy-datatables>`_


License
-------

Copyright (c) 2013 Chris Strutton
Portions Copyright (c) 2013 Michel Nemnom

Permission is hereby granted, free of charge, to any person
obtaining a copy of this software and associated documentation
files (the "Software"), to deal in the Software without
restriction, including without limitation the rights to use, copy,
modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.