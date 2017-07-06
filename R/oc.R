cl.insert <- function(id, ts, attr, values, cluster="hadoop220") .Call(OC_insert, cluster, id, ts, attr, values)

cl.prepare <- function(query, cluster="hadoop220") .Call(OC_query_prepare, cluster, query)
cl.execute <- function(query, result.colspec = 8L, ..., wait=TRUE) .External(if (wait) OC_query_exec else OC_query_exec_async, query, result.colspec, ...)
cl.is.ready <- function(query) .Call(OC_query_is_ready, query)
cl.collect <- function(query) .Call(OC_query_collect, query)
