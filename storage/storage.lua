local log = require('log')
netbox = require('net.box')

box.cfg{listen = 3301}

box.execute("drop table if exists t")
box.execute("CREATE TABLE t (id int primary key, a int, b int);");
box.execute("insert into t values (1, 1, 1)");

box.schema.user.grant('guest', 'super', nil, nil, {if_not_exists = true})

function test_const(arr)
    log.info("Calling test_const with arr=%s", arr)
    return 42
end

function test_select(arr)
    log.info("Calling test_select with arr=%s", arr)
    return box.space.T:select()
end
