net_box = require('net.box')
fiber = require('fiber')
env = require('test_run')
test_run = env.new()

box.schema.user.grant('guest', 'replication')
box.schema.user.grant('guest','read,write,execute,create,drop','universe')
test_run:cmd("create server test with rpl_master=default, script='replication/replica_timeout.lua'")
test_run:cmd("start server test with args='3'")

_ = box.schema.space.create('counter')
_ = box.space.counter:create_index('primary')

test_run:cmd("switch test")
net_box = require('net.box')
fiber = require('fiber')
test_run:cmd("set variable default_uri to 'default.listen'")
con1 = net_box.connect(default_uri)
function check(t) fiber.create(function() fiber.sleep(t) con1:call('box.space.counter:auto_increment', {{t}}) end) end

test_run:cmd("switch default")
test_run:cmd("set variable test_uri to 'test.listen'")
test_con = net_box.connect(test_uri)
graceful_con = net_box.connect(test_uri)
graceful_con.space._session_settings:update('graceful_shutdown', {{'=', 2, true}})
graceful_con.space._session_settings:select{}
inserted_num = 0
graceful_con:shutdown()
for time = 1, 10, 2 do inserted_num = inserted_num + 1 test_con:call('check', {time}) end

fiber.sleep(1)
test_run:cmd("stop server test")

graceful_con:ping()
net_box.connect(test_uri):ping()
test_con:ping()

fiber.sleep(3)
box.space.counter:select()
inserted_num

test_run:cmd("delete server test")
box.space.counter:drop()
box.schema.user.revoke('guest', 'replication')
box.schema.user.revoke('guest','read,write,execute,create,drop','universe')
