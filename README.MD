Test function for MongoDB.

Configure following env variables for your function app settings:

- MONGO_URI connection string for your mongodb server including secret
- MONGO_DB the database in your mongodb server
- MONGO_COLLECTION the collection in your database
- MONGO_FIELD_1 Field name of one of the documents in your database
- MONGO_VALUE_1 Value of property in a document, used in query find ( eq (MONGO_FIELD_1, MONGO_VALUE_1) )

