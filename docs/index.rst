.. sqlquery documentation master file, created by
   sphinx-quickstart on Mon Jan 19 17:13:27 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

sqlquery - an SQL translation layer
===================================

Release v\ |version|.

sqlquery is a SQL translation library,
written in Python. The main goal is to provide a python-like interface to an
implementation that outputs a composed query and a set of arguments that can be
used with a SQL client library (such as PyMYSQL).

It's currently in pre-alpha mode and as such is not feature complete nor
thoroughly production tested.

::


    >>> from sqlquery.queryapi import select
    >>> select("id").on_table("users").where(("id__eq", 2)).sql()
    (u'SELECT `a`.`id` FROM `users` AS `a` WHERE (`a`.`id` <=> %s)', (2,))


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


API Documentation
-----------------

.. toctree::
   :maxdepth: 2

   api
