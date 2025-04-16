# hbase shell
list

scan 'test_table'

alter 'my_table', {NAME => 'cf1'}
alter 'my_table', {METHOD => 'delete', NAME => 'cf1'}

truncate 'my_table'

count 'my_table'