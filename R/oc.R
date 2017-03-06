cl.insert <- function(id, ts, attr, values, cluster="hadoop220") .Call(OC_insert, cluster, id, ts, attr, values)

cl.prepare <- function(query, cluster="hadoop220") .Call(OC_query_prepare, cluster, query)
cl.execute <- function(query, result.colspec = 8L, ...) .External(OC_query_exec, query, result.colspec, ...)
