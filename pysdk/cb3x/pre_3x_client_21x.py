#!/usr/bin/env python
from __future__ import print_function

from pprint import pprint

from couchbase.bucket import Bucket
from couchbase.n1ql import N1QLQuery
from couchbase.exceptions import NotFoundError

#cb = Bucket('couchbase://localhost/default')
cb = Bucket('http://13.56.228.134:8091/default')
#cb = Bucket('couchbase://localhost/ecommerce')
print("CB Server node in this cluster are [" + str(cb.server_nodes) + "]")
print("CB Server connection PASSED")
#cb.upsert('u:king_arthur', {'name': 'Arthur', 'email': 'kingarthur@cb.com', 'interests' : ['Holy Grail', 'Kingdoms']})
#cb.upsert('u:king_arthur', 'Rule the England', replicate_to=0)

print('Upserting...')
cb.upsert('u:king_arthur', {'name' : 'K Arthur', 'email' : 'arthur@cb.com', 'type' : 'Royales', 'interests' : ['Braids', 'Hunting']})
print('Done...')

# get non-existent key
print('Getting non-existent key. Should fail..')
try:
        cb.get('get-non-existent')
except NotFoundError:
        print('Got exception for missing doc')

print('Inserting...')
cb.insert('u:queen_liz', {'name': 'Liz', 'email': 'liz@cb.com', 'type' : 'Royales', 'interests' : ['Holy Grail', 'Kingdoms']})
print('Done...')

# get non-existent key
print('Getting an existent key. Should pass...')
try:
        print("Value for key 'queen_liz'\n")
        val = cb.get('u:queen_liz').value
        print("Value for key 'queen_liz'\n%s" %(val))
except NotFoundError as e:
        print('Got exception for missing doc with error %s' %(e))

# create primary index
print('Create a primary index...')
#mgr = cb.bucket_manager()
#mgr.n1ql_index_create_primary(ignore_exists=True)
#cb.n1ql_query('CREATE PRIMARY INDEX ON default').execute()
print('Done...')

# Fire a n1ql query
#print('Fire a N1QL query...')
#query = N1QLQuery("SELECT name, email FROM `default` WHERE type=\"Royales\"")
#for row in cb.n1ql_query(query):
#    pprint(row)

print('Closing connection to the bucket...')
Bucket._close(cb)
