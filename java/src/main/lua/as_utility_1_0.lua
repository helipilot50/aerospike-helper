
-- Print contents of `tbl`, with indentation.
-- `indent` sets the initial level of indentation.
function dumpTable (tbl, indent)
  if not indent then indent = 0 end
  for k, v in pairs(tbl) do
    formatting = string.rep("  ", indent) .. k .. ": "
    if type(v) == "table" then
      debug(formatting)
      dumpTable(v, indent+1)
    else
      debug(formatting .. tostring(v))
    end
  end
end
-- debug routing to print the local heap

function dumpRecord (rec)
  local record_string = "\nrecord:\n"
  record_string = record_string.."  set:"..tostring(record.setname(rec)).."\n"
  record_string = record_string.."  digest:"..tostring(record.digest(rec)).."\n"
  record_string = record_string.."  gen:"..tostring(record.gen(rec)).."\n"
  record_string = record_string.."  exp:"..tostring(record.ttl(rec)).."\n"
  record_string = record_string.."  bins:".."\n"
  local names = record.bin_names(rec)
  for k, v in pairs(names) do
    local binVal = rec[v]
    record_string = record_string.."    (" .. v .. ":"..tostring(binVal)..")\n"
  end
  debug(record_string)
end

function dumpLocal()
  local i = 1 
  repeat
        local name, value = ldebug.getlocal(2, i)
        if name then 
          if type(value) == "table" then
            debug("dump:"..name)
            dumpTable(value, 1)
          else
            debug("dump:"..name.." = "..tostring(value)) 
          end
        end
        i = i + 1
  until not name
end

function listContains(collection, value)
    for v in list.iterator(collection) do
      if v == value then
        return true
      end
    end
  return false
end 

function listRange(collection, low, high)
    for v in list.iterator(collection) do
      if v >= low and v <= high  then
        return true
      end
    end
  return false
end 

function containsKey(collection, key)
	for k,_ in map.pairs(collection) do
	  if k == value then
	    return true
	  end
	end
	return false
end 

function containsValue(collection, value)
    for _,v in map.pairs(collection) do
      if v == value then
        return true
      end
    end
  return false
end 

function rangeValue(collection, low, high)
	  for _,v in map.pairs(collection) do
	    if v >= low and v <= high  then
	      return true
	    end
	  end
	return false
end 

function rangeKey(collection, low, high)
	for k,_ in map.pairs(collection) do
	  if k >= low and k <= high  then
	    return true
	  end
	end
	return false
end 


local function filter_record(rec, filterFuncStr, filterFunc)
  -- dumpRecord(rec)
  -- if there is no filter, select all records
  if filterFuncStr == "none" then
    return true
  end
  debug("filterFuncStr:"..filterFuncStr)
  
  -- if there was a filter specified, and was successfully compiled
  if filterFunc ~= nil then
    local context = {rec = rec, 
                    selectedRec = false, 
                    string = string, 
                    generation = record.gen(rec),
                    digest = record.digest(rec),
                    set_name = record.setname(rec),
                    expiry = record.ttl(rec),
                    listContains = listContains,
                    listRange = listRange,
                    rangeKey = rangeKey,
                    rangeValue = rangeValue,
                    containsKey = containsKey,
                    containsValue = containsValue,
                    table = table,
                    dumpLocal = dumpLocal,
                    dumpTable = dumpTable,
                    dumpRecord = dumpRecord,
                    debug = debug,
                    info = info}

    -- sandbox the function
    setfenv(filterFunc, context)
    filterFunc()
    --dumpLocal()
    return context.selectedRec
  end

  -- if there was a filter function, but failed to compile
  return true
end

local function parseFieldStatements(fieldValueStatements)
  local fieldFuncs = nil
  if fieldValueStatements ~= nil then
    fieldFuncs = {}
    for fn, exp in map.pairs(fieldValueStatements) do
      fieldFuncs[fn] = load(exp)
    end
  end
  return fieldFuncs
end


function delete_record(rec)
  if aerospike:exists(rec) then
    aerospike:remove(rec)
  end
end
------------------------------------------------------------------------------------------
--  Returns Maps For Specified Filters
------------------------------------------------------------------------------------------

function select_records(stream, origArgs)
  debug("origArgs: "..tostring(origArgs))
  local filterFuncStr = origArgs["filterFuncStr"]
  local fieldValueStatements = origArgs["funcStmt"]
  local fields = origArgs["selectFields"]
  
  local includeAllFields = false
  if origArgs["includeAllFields"] == 1 or origArgs["includeAllFields"] == 'true' then
    includeAllFields = true
  end

  local filterFunc = nil
  if filterFuncStr ~= "none" then
  --  debug("load filter function:"..tostring(filterFuncStr))
    filterFunc = load(filterFuncStr)
  --  debug("loaded filter function")
  end
  local fieldFuncs = parseFieldStatements(fieldValueStatements)

  local function map_record(rec)

    -- Could add other record bins here as well.
    -- This code shows different data access to record bins
    local result = map()
    local addAllFields = false

    if fields ~= nil then
      for v in list.iterator(fields) do
        if fieldFuncs ~= nil and fieldFuncs[v] ~= nil then
          local context = {rec = rec, result = nil}
          local f = fieldFuncs[v]
          -- sandbox the function
          setfenv(f, context)
          f()

          result[v] = context.result
        else
          result[v] = rec[v]
        end
      end
    end

    if (fields == nil) or (includeAllFields == true) then
      local names = record.bin_names(rec)
      for i, v in ipairs(names) do
        --debug(tostring(rec[v]))
        result[v] = rec[v]
      end
    end
    result["meta_data"] = map()
    result["meta_data"]["digest"] = record.digest(rec)
    result["meta_data"]["generation"] = record.gen(rec)
    result["meta_data"]["set_name"] = record.setname(rec)
    result["meta_data"]["expiry"] = record.ttl(rec)
    return result
  end


  local function filter_records(rec)
    --debug("filterFuncStr:"..tostring(filterFuncStr))
    return filter_record(rec, filterFuncStr, filterFunc)
  end

  if filterFuncStr ~= "none" then
    return stream : filter(filter_records) : map(map_record)
  else
    return stream : map(map_record)
  end
end

------------------------------------------------------------------------------------------
--  Returns Record Meta For Specified Filters
------------------------------------------------------------------------------------------
function query_meta(stream, origArgs)
  debug("origArgs: "..tostring(origArgs))
  local filterFuncStr = origArgs["filterFuncStr"]

  local filterFunc = nil
  if filterFuncStr ~= "none" then
    filterFunc = load(filterFuncStr)
  end

  local function add_records(rec)

    local result = map()
    result["meta_data"] = map()
    result["meta_data"]["digest"] = record.digest(rec)
    result["meta_data"]["generation"] = record.gen(rec)
    result["meta_data"]["set_name"] = record.setname(rec)
    result["meta_data"]["expiry"] = record.ttl(rec)
      
    return result
  end

  local function filter_records(rec)
    return filter_record(rec, filterFuncStr, filterFunc)
  end

  return stream : filter(filter_records) : map(add_records)
end

------------------------------------------------------------------------------------------
--  Returns All bin names
------------------------------------------------------------------------------------------
function query_bin_names(stream, origArgs)
  debug("origArgs: "..tostring(origArgs))
  local filterFuncStr = origArgs["filterFuncStr"]

  local filterFunc = nil
  if filterFuncStr ~= "none" then
    filterFunc = load(filterFuncStr)
  end

  local function map_bin_names(bin_names, rec)

      local names = record.bin_names(rec)
      for i=1, #names do 
        bin_names[names[i]] = 0
      end

      return bin_names
  end

  local function reducer(a, b)
    local res = map.merge(a, b)
    return res
  end

  local function filter_records(rec)
    return filter_record(rec, filterFuncStr, filterFunc)
  end

  return stream : filter(filter_records) : aggregate(map {}, map_bin_names) : reduce(reducer)
end

-----------
