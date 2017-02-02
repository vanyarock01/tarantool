#!/usr/bin/env tarantool

env = require('test_run')
test_run = env.new()

page_size = 1024

space = box.schema.space.create('test', { engine = 'vinyl' })
pk = space:create_index('primary', { page_size = page_size, range_size = page_size })

--
-- gh-1778: Create ranges with one page and try to select from it
--
test_run:cmd("setopt delimiter ';'")
l = string.rep('a', page_size)
for i = 0,4 do
    space:replace({i})
    box.snapshot()
end
test_run:cmd("setopt delimiter ''");
#space:select()

k = 0
for _, t in space:select() do k = k + t[1] end
k

test_run:cmd("setopt delimiter ';'")
k = 0;
for i = 0,4  do
    t = space:get(i)
    k = k + t[1]
end;
test_run:cmd("setopt delimiter ''");
k

space:drop()
