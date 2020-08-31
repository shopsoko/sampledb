## Sampledb

### Usage

    ./sampledb -driver=dbdriver -host=dbhost -port=port -user=user -pass=pass -targetschema=targetschema -sampleschema=sampleschema -anchor=table_name#col=val,val -nosample=tbl1,tbl2


```

Usage of sampledb:

  -driver string
    	db driver (default "mysql")

  -host string
    	db host (default "localhost")

  -pass string
    	db user pass (default "root")

  -port string
    	db port (default "3306")

  -user string
    	db user (default "root")

  -nosample string
    	comma separated list of tables name which will be copied in full

  -sampleschema string
    	sample schema name (default "defaults to sample_db_{secs since January 1, 1970 UTC}")

  -targetschema string
    	target schema name

  -anchor string
    	table from which we'll start looking fot relationships, you can prepend a # followed by a list of comma separated ids after the table name to get specific rows only, otherwise we'll randomly select 5 rows.
    	for example: 
    		-anchor=table#column=value,value,value
  

```