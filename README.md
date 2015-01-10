phpcb-migration
===============

PHP Class to provide v1 client lib methods with the v2 lib

Between the version 1 and 2 of the couchbase PHP client library the Couchbase class was replaced.

The class in this project aims at being a drop in replacement to make existing call to the v1 Couchbase object still work when the v2 lib is installed.


Note that the PHP client library v2 needs the C library : libcouchbase version 2.4.5 (at least).

It works with PHP 5.3 (tested with 5.3.9) and higher.

This class is intended to be used with the PHP client library 2.0.3 or higher. In 2.0.2 and before two bugs existed, to use this version of the PHP client library see the 2.0.2 tag.

It might differ a little from certain return codes with the v1 PHP client library as those were not very well documented.

Its status is : "ready to use with the usual precaution". Some PHP doc might be added in the future...


---

Usual disclaimer : do not put it in production environment without extensive testing. This class is provided as is and free to use.
