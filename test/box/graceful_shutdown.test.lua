net_box = require('net.box')
fiber = require('fiber')
env = require('test_run')
test_run = env.new()

--guest can have some of these privileges
_, _ = pcall(function() box.schema.user.grant('guest','execute,write,read','universe') end)

test_run:cmd("create server test with script='replication/replica_timeout.lua'")
test_run:cmd("start server test")

_ = box.schema.space.create('counter')
_ = box.space.counter:create_index('primary')

test_run:cmd("switch test")
_, _ = pcall(function() box.schema.user.grant('guest','execute,write,read','universe') end)
net_box = require('net.box')
fiber = require('fiber')
test_run:cmd("set variable default_uri to 'default.listen'")
def_con = net_box.connect(default_uri)
function check(t) fiber.sleep(t) def_con:call('box.space.counter:auto_increment', {{'sleep ' .. tostring(t)}}) end
function exit() require('fiber').new(function() os.exit() end) return 'exit scheduled' end

test_run:cmd("switch default")
test_run:cmd("set variable test_uri to 'test.listen'")
test_con = net_box.connect(test_uri)
graceful_con = net_box.connect(test_uri)
graceful_con.space._session_settings:update('graceful_shutdown', {{'=', 'value', true}})
graceful_con.space._session_settings:select{}
graceful_con:set_shutdown_handler(function() box.space.counter:auto_increment{'shutdown receive'} end)
for time = 0, 10, 2 do test_con:call('check', {time}, {is_async=true}) end

-- Default on_shutdown triggers timeout == 3, but we sets it
-- here directly to make test clear
box.ctl.set_on_shutdown_timeout(3)
status, error = pcall(function() test_con:call('exit') end)
fiber.sleep(1)

net_box.connect(test_uri).state
test_con:ping()
graceful_con:ping()
_ = graceful_con:shutdown()
graceful_con:ping()

fiber.sleep(4)
box.space.counter:select()

box.space.counter:drop()
test_run:cmd("delete server test")
box.schema.user.revoke('guest','execute,write,read','universe')