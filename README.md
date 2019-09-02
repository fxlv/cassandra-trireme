# cassandra-row-counter
How many rows do you have in a table?

## Why?

Sometimes it is handy to know how many rows of data you have in your Cassandra database.
Unless you've thought about this when designing your data model, there really is not much choice then to run a full table scan 
by doing ```select count(*) from keyspace.table```.

However that will most likely time out.
At least for me, when tables are over few million, the count always times out.

But what if you really need to know the row count.
Well, then what you can do is to do the counting in smaller batches.
And that is exactly what this script here does.

## Current status

This is very much work in progress, currently it works, but requires you to hardcode all the options in the script.
On the todo list is to add SSL support and argument support via CLI.
Feel free to jump in if you wish.
