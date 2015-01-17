.. _api:

Developer Interface
===================

.. module:: sqlquery.queryapi

This part of the documentation covers most of the interfaces of sqlquery.


Main Interface
--------------
You usually will start with one of these functions. Each will return an
instance of :py:class:`QueryBuilder`

.. autofunction:: select
.. autofunction:: update
.. autofunction:: insert
.. autofunction:: insert_ignore
.. autofunction:: replace
.. autofunction:: delete


QueryBuilder
~~~~~~~~~~~~

.. autoclass:: QueryBuilder
   :inherited-members:

Exceptions
~~~~~~~~~~

.. autoexception:: sqlquery.queryapi.InvalidQueryException


Licensing
~~~~~~~~~

.. _Apache 2.0: http://opensource.org/licenses/Apache-2.0
