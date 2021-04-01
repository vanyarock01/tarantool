-- setup variables
results_replica = '0'
results_master = '0'
bsize = 1000
loops = 100

-- cleanup tuples
--for id = 1,bsize*loops do spec_delete(box.space.test, id) end

-- insert/delete tuples
start_pos = #box.space.test:select()
for num = 1,loops do
    bpos = start_pos + bsize * (num - 1)
    for id = 1,bsize do spec_insert(box.space.test, bpos + id) end
    for id = 1,bsize,num do spec_delete(box.space.test, bpos + id) end
    -- collect statistics on replica
    results_replica = results_replica .. ', ' .. c:eval([[ return #box.space.test:select() ]])
    -- collect statistics on master
    results_master = results_master .. ', ' .. #box.space.test:select()
end
