py-sql-query
============

py-sql-query is a basic SQL translation layer in
python. You construct queries using mainly python constructs which later can be
serialized to a SQL query.

Examples
--------

A simple select statement
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    >>> from sqlquery.queryapi import select
    >>> select("id").on_table("users").where(("id__eq", 2)).sql()
    (u'SELECT `a`.`id` FROM `users` AS `a` WHERE (`a`.`id` = %s)', (2,))

A simple update statement
~~~~~~~~~~~~~~~~~~~~~~~~~
.. code-block:: python

    >>> from sqlquery.queryapi import update
    >>> update(username="john").on_table("users").where(("id__eq", 2)).sql()
    (u'UPDATE `users` AS `a` SET `a`.`username` = %s WHERE (`a`.`id` = %s)', ('john', 2))

A simple insert statement
~~~~~~~~~~~~~~~~~~~~~~~~~
.. code-block:: python

    >>> from sqlquery.queryapi import insert
    >>> insert(username="john", id=4).on_table("users").sql()
    (u'INSERT INTO `users` (`username`, `id`) VALUES (%s, %s)', ('john', 4))

A simple order by statement
~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. code-block:: python

    >>> from sqlquery.queryapi import select
    >>> select("id").on_table("users").order_by("id").sql()
    (u'SELECT `a`.`id` FROM `users` AS `a` ORDER BY `a`.`id`', ())

A subquery
~~~~~~~~~~
.. code-block:: python

    >>> from sqlquery.queryapi import select
    >>> select(
          "username", "id"
        ).on_table("users").where(
          ("id__in", select("id").on_table("banned_users"))
        ).sql()
    (u'SELECT `a`.`username`, `a`.`id` FROM `users` AS `a` WHERE (`a`.`id` IN (SELECT `b`.`id` FROM `banned_users` AS `b`))',
     ())


A more involved statement
~~~~~~~~~~~~~~~~~~~~~~~~~
.. code-block:: python

    >>> from sqlquery.queryapi import select
    >>> select(
          "username", "id", "contactinfo.address"
        ).on_table("users").join(
          "contactinfo",
          "id",
        ).where(
          ('id__in', [1, 2, 3, 4]), ('contactinfo.country__eq', 'US')
        ).limit(10).offset(10).order_by("id").sql()
    (u'SELECT `a`.`username`, `a`.`id`, `b`.`address` FROM `users` AS `a` INNER JOIN `contactinfo` AS `b` ON `a`.`id` = `b`.`id` WHERE (`a`.`id` IN (%s,%s,%s,%s)) AND (`b`.`country` = %s) ORDER BY `a`.`id` OFFSET %s LIMIT %s',
     (1, 2, 3, 4, 'US', 10, 10))

