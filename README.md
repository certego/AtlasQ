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



## Usage
Now the most important part: how do you use this package?


```python3
from mongoengine import Document, fields

from atlasq import AtlasManager, AtlasQ, AtlasQuerySet

index_name = str("my_index")

class MyDocument(Document):
    name = fields.StringField(required=True)
    surname = fields.StringField(required=True)
    atlas = AtlasManager(index_name)

obj = MyDocument.objects.create(name="value", surname="value2")

qs = MyDocument.atlas.filter(name="value")
assert isinstance(qs, AtlasQuerySet)
obj_from_atlas = qs.first()
assert obj == obj_from_atlas

obj2_from_atlas = MyDocument.atlas.get(AtlasQ(name="value") & AtlasQ(surname="value2"))
assert obj == obj2_from_atlas


obj3_from_atlas = MyDocument.atlas.get(AtlasQ(wrong_field="value"))
assert obj3_from_atlas is None

result = MyDocument.atlas.ensure_index("user", "pwd", "group", "cluster")
assert result is True
obj3_from_atlas = MyDocument.atlas.get(AtlasQ(wrong_field="value")) # raises AtlasIndexFieldError



```

##  Extended Features

### Validation
We also decided to have, optionally, a validation of the index.
Two things are checked:
- The index actually exists (If you query a non-existing index, Atlas as default behaviour will not raise any error).
- The fields that you are querying are actually indexed(If you query a field that is not indexed, Atlas as default behaviour will not raise any error, and will return an empty list).
To make these check, you need to call the function `ensure_index` on the queryset:

### EmbeddedDocuments
Embedded documents are queried in two different ways, depending on how you created your Search Index.
Remember to ensure the index so that AtlasQ can know how your index is defined
If you used the [embeddedDocuments](https://www.mongodb.com/docs/atlas/atlas-search/define-field-mappings/#std-label-bson-data-types-embedded-documents) type, AtlasQ will use the [embeddedDocument](https://www.mongodb.com/docs/atlas/atlas-search/embedded-document/) query syntax.
Otherwise, if you used the [document](https://www.mongodb.com/docs/atlas/atlas-search/define-field-mappings/#document) type, or you did not ensure the index, a normal `text` search with the `.` syntax will be used.

Given a Collection as:
```python3
from mongoengine import Document, EmbeddedDocument, EmbeddedDocumentListField, fields

class MyDocument(Document):
    class MyEmbeddedDocument(EmbeddedDocument):
        field1 = fields.StringField(required=True)
        field2 = fields.StringField(required=True)
    
    list = EmbeddedDocumentListField(MyEmbeddedDocument)    

```
and given the following document in the collection
```python3

MyDocument(list=[MyEmbeddedDocument(field1="aaa", field2="bbb"), MyEmbeddedDocument(field1="ccc", field2="ddd")])
MyDocument(list=[MyEmbeddedDocument(field1="aaa", field2="ddd"), MyEmbeddedDocument(field1="ccc", field2="bbb")])
```
the following query will retrieve both the documents, instead of only the first
```python3
assert MyDocument.objects.filter(list__field1="aaa", list__field2="bbb").count() == 2

```
This is done because each clause will check that `one` document match it, not the these condition must be on the same object.

To solve this, inside AtlasQ, if you write multiple condition that refer to the same EmbeddedObject in a *single* AtlasQ
object, all the condition must match a single object; if the conditions are in multiple AtlasQ object, the default behaviour will be used

```python3
assert MyDocument.atlas.filter(list__field1="aaa", list__field2="bbb").count() == 1
assert MyDocument.atlas.filter(AtlasQ(list__field1="aaa")& AtlasQ(list__field2="bbb")).count() == 2
```

