# AtlasQ
AtlasQ allows the usage of [AtlasSearch](https://www.mongodb.com/docs/atlas/atlas-search/) keeping the [MongoEngine](https://github.com/MongoEngine/mongoengine) syntax.

## Structure
The package tries to follow the MongoEngine structure;
the major differences reside in the `transform.py` and `queryset.py` files. 

### Transform
Like in MongoEngine, a step in the pipeline is the creation of a query from a `Q` object: 
we have to find a correspondence between the MongoEngine common syntax and what AtlasSearch allows.
For doing this, we had to find some compromises.

Not every keyword is supported at the moment: if you have an actual use case that you would like to support,
please be free to open an issue or a PR at any moment.

### QuerySet
There are probably a thousand of better implementation, if you actually knew MongoEngine and above all [PyMongo](https://pymongo.readthedocs.io/en/stable/).
Unfortunately, our knowledge is limited, so here we go. If you find a solution that works better, again, feel free to open an issue or a PR.

The main idea, is that the `filter` should work like an `aggregation`. 
For doing so, and with keeping the compatibility on how MongoEngine works (i.e. the filter should return a queryset of `Document`) we had to do some work.  
Calling `.aggregate` instead has to work as MongoEngine expect, meaning a list of dictionaries. 
#### Features
##### Cache
To complicate things, we even decided to add a cache!
The issue there is that we do query on a large collection, and the query is composed of many clauses, meaning that
the query could take 2/3 minutes to have a result. The second constraint is that the result of this query does not change rapidly 
(or at least not the `filtering` part). If the cache is enabled, we save the objects retrieved in a new temporary collection
and future queries that checks the same parameters, will retrieve the documents from this new collection.

##### Validation
We also decided to have, optionally, a validation of the index.
Two things are checked:
- The index actually exists (If you query a non-existing index, Atlas as default behaviour will not raise any error).
- The fields that you are querying are actually indexed(If you query a field that is not indexed, Atlas as default behaviour will not raise any error, and will return an empty list).
To make these check, you need to call the function `ensure_index` on the queryset:


## Usage
Now the most important part: how do you use this package?


```python3
from mongoengine import Document, fields

from atlasq import AtlasManager, AtlasQ, AtlasQuerySet

class MyDocument(Document):
    name = fields.StringField(required=True)
    surname = fields.StringField(required=True)
    atlas = AtlasManager("myindex", "default")

obj = MyDocument.objects.create(name="value", surname="value2")

qs = MyDocument.atlas.filter(name="value")
assert isinstance(qs, AtlasQuerySet)
obj_from_atlas = qs.first()
assert obj == obj_from_atlas

obj2_from_atlas = MyDocument.atlas.get(AtlasQ(name="value") & AtlasQ(surname="value2"))
assert obj == obj2_from_atlas
```