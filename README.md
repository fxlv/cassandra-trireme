# cassandra-trireme

Count, print and manipulate rows.
And do that in a distributed way.

## Why?

This tool solves two main problems.

### Counting rows

Sometimes it is handy to know how many rows of data you have in your Cassandra database.
Unless you've thought about this when designing your data model, there really is not much choice then to run a full table scan 
by doing ```select count(*) from keyspace.table```.

However that will most likely time out.
At least for me, when tables are over few million, the count always times out.

But what if you really need to know the row count.
Well, then what you can do is to do the counting in smaller batches.
And that is exactly what this script here does.


### Updating rows

Sometimes you need to manipulate your data and say something like:

```update test1.testtable1 set something = "new value" where another_criteria = "something"```

You might expect this to work, but it won't. Cassandra won't allow you to do that unless you're filtering by primary key.
That's less than ideal, and we can talk about data modeling all day, but reality is that in some cases you just need to run this one query.

And that is the other key thing that this tool allows you to do.

## User guide

### Installing

Not much to do here, it _just works_. 
Tested on Linux and Windows, Python 3.4 - Python 3.7.

You do need to install `cassandra-driver` python module first though.

### A sidenote on SSL

This tool optionally supports SSL, just pass the necessary options to it.

For example, running without SSL:

```./count.py count-rows 172.17.86.138 test1 testtable2 id```

With SSL (certificate and key must be in PEM format):

```./count.py count.py count-rows 172.17.86.138 test1 testtable2 id --ssl-certificate client.cer.pem --ssl-key key.pem```

For brevity, I will omit the SSL arguments in most examples.

### Required positional arguments

The tool supports multiple actions, such as counting, updating etc.
For these operations to work, you will have to provide some arguments.

|Argument| Description |
| --- | --- |
| host |                  Cassandra host. This can be hostname or IP.|
|keyspace        |      Keyspace to use|
|table                 |Table to use|
|key|                   Key to use. This will be used to calculate token ranges, this should be your primary key.|
  
  
### Counting rows

Counting rows is not something that Cassandra is designed for. But hey, sometimes you just really need this.
Say, you want to do somethig like: ``` select count(*) from test1.testtable2;```. 
This will most likely not work for you, as it is inefficient, and takes time. Cassandra will time out. By default the timeout is 5 sec IIRC.

Solution:
```
./count.py count-rows 127.0.0.1 test1 testtable2 id
```

Please note the `id` here. This is the primary key that we use to compute token ranges.

Complete example:
```
C:\Python37\python.exe count.py count-rows 172.17.86.138 test1 testtable2 id
Total amount of rows in test1.testtable2 is 8
```

#### Adding filtering conditions
Now what if you want to ask a different question, like: `select count(*) from test1.testtable2 where some_column = something`.
This is where `--filter-string` option comes in. It essentially adds a `where x = z` clause.

Let's look at this example:
```
 C:\Python37\python.exe count.py count-rows 127.0.0.1 test1 testtable2 id --filter-string="name = 'fx2' allow filtering"
```

Here we are actually creating a query (token ranges will be different):
```
select count(*) from test1.testtable2 where token(id) >= 6776627963145224192 and token(id) < 7776627963145224192 and name = 'fx2' allow filtering
``` 

Please note the `ALLOW FILTERING` part. This is needed if you want to filter on a column that is not indexed.
This is not ideal, and not safe, but it does get the job done.

### Printing rows

This is very similar to counting, but, say, you want to actually print them out.
It would be the same as doing `select * from test1.testtable2 where some_column = something`.

Example:
```
 C:\Python37\python.exe count.py print-rows 127.0.0.1 test1 testtable2 id --filter-string="name = 'fx2' allow filtering"
```

If selecting `*` is a bit too much, you can also restrict it to specific columns by using the `--value-column` option like so:

```
count.py print-rows 127.0.0.1 test1 testtable2 id --filter-string="name = 'fx2' allow filtering" --value-column=name
```
This will print only your `id` (primary key) and the column you specified.

### Updating rows

Sometimes you need to manipulate rows.
This is same as if you'd want to do: `update test1.testtable2 set name='Olympius' where name = 'fx2';`.
It won't work, as, again, it would require a full table scan.
The solution, at least one of solutions, is to do a query to find all rows that match the criteria and then do an update specifically for them.

Let's look at an example:
```
C:\Python37\python.exe count.py update-rows 127.0.0.1 test1 testtable2 id --filter-string="name = 'fx1' allow filtering"  --debug --update-key=name --update-value='Olympius'
```

This will do two things, first it will scan all rows and look for rows where `name = 'fx1'` and note down their `id`.
Then on second pass it will iterate over the collected set of `id's` and update the column `name` with the new value `Olympius`.

This has been tested with integers, strings and booleans. The script will be clever enough to quote strings in the underlying sql requests.

#### But what if you have compound primary key?

Yes, that is very likely the case in Cassandra world, and it that very case you can provide an extra key by using `--extra-key=registrationid` option.

Example:
```
C:\Python37\python.exe count.py update-rows 127.0.0.1 test1 testtable2 id --filter-string="name = 'Olympius' allow filtering"  --debug --update-key=name --update-value='Olympius' --extra-key=second_id
``` 
This will do same as the above version, but in the collection phase it will collect both keys, so that during update phase it knows how to find your data.

The update operation will also ask for human confirmation before doing the update run, so it is somewhat _safe_ to run it.
But of course - _caveat emptor_.



### Additional information

#### Multi threading

It uses 15 threads by default. 
I forgot to implement an option for that, so you can just search for `thread_count` in the code if you want to change it.
Or better yet - make a PR and fix it :)


#### Debugging

You can add `--debug` option to get tons of debugging information printed to your `stdout`.


#### Split size

Cassandra uses 2^64 partitions, and the complexity of doing the full scan lies in the fact that it has to query all of them to get the row count.
Now, there might be a better way, but currently seems easiest is just to do many queries by specifying particular range of tokens.
This is similar to how Cassandra Reaper does it.

Very simple approach that seems to work for me is to divide the 2^64 range by number in that is a power of 10.
So, for example if I use 10^18, this divides the 2^64 nicely into 18 splits.
You can experiment with this number and the bigger the table the smaller number you can specify.
So, for example for a 4M+ table, number 10^15 works well.

This is something to improve, and possible some sort of auto detection could be done.
For now, default is 10^18, and if you need smaller splits, you can specify smaller [powers of 10](https://en.wikipedia.org/wiki/Power_of_10).

You can override defauls split size with the `--split` option. For best results use powers of 10.

## Current status

This is very much work in progress, currently it works, but isn't pretty.
Feel free to jump in if you wish.

## The name - Trireme

As this tool mainly deals with different aspects of row counting and manipulation, then it seems rowing motive might be in order.
And keeping inline with Cassandra, Trireme is a type of ancient greek galley, with three rows of oars. So lots of rowing.
Hence the name.
