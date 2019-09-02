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

## Running
Without SSL:

```./count.py 127.0.0.1  --user testuser --pass testpass```

With SSL (certificate and key must be in PEM format):

```./count.py 127.0.0.1 system_auth roles role --port 9142 --user testuser --pass testpass --ssl-certificate client.cer.pem --ssl-key key.pem```

## Split size

Cassandra uses 2^64 partitions, and the complexity of doing the full scan lies in the fact that it has to query all of them to get the row count.
Now, there might be a better way, but currently seems easiest is just to do many queries by specifying particular range of tokens.
This is similar to how Cassandra Reaper does it.

Very simple approach that seems to work for me is to use divide the 2^64 range by number in that is a power of 10.
So, for example if I use 10^18, this divides the 2^64 nicely into 18 splits.
You can experiment with this number and the bigger the table the smaller number you can specify.
So, for example for a 4M+ table, number 10^15 works well.

This is something to improve, and possible some sort of auto detection could be done.
For now, default is 10^18, and if you need smaller splits, you can specify smaller [powers of 10](https://en.wikipedia.org/wiki/Power_of_10).

## Current status

This is very much work in progress, currently it works, but requires you to hardcode all the options in the script.
On the todo list is to add SSL support and argument support via CLI.
Feel free to jump in if you wish.
