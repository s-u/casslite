\name{cl.query}
\alias{cl.query}
\alias{cl.prepare}
\alias{cl.execute}
\title{
  Prepare and execute CSQL Queries
}
\description{
  \code{cl.prepare} connects to a Cassandra cluster and creates prepared
  CSQL query on the cluster.

  \code{cl.execute} binds parameters (if specified), executes a prepared
  query and return the result.
}
\usage{
cl.prepare(query, cluster = "hadoop220")
cl.execute(query, result.colspec = 8L, ...)
}
\arguments{
  \item{query}{For \code{cl.prepare} this is a string defining the
  query. For \code{cl.execute} this is the object returned by
  \code{\link{cl.prepare}}.
}
\item{cluster}{Hostname to connect to}
\item{result.colspec}{integer, specificaltion of the result types as a
  bitmask. If 0 then no result is retrieved (for DDL queries). If -1
  then a single raw value is expected in the result, otherwise a bitmask
  corresponding to id-string[1], ts-timestamp[2], attr-string[4],
  value-string[8].
  Note that this is currently a horrible hack simply becasue of where
  this code originally comes from which will change to something more
  sane eventually.}
}
\details{
  \code{cl.prepare} connects to the cluster and compiles the query which
  can be parametrized using the usual "?" wildcard parameters. Those
  can be then bound at execution time. The result is a reference to the
  live connections object which can be used in subsequent
  \code{cl.execute} calls. The connection is only closed after the
  objects goes out of scope (by the virtue of its finalizer).

  \code{cl.execute} binds any parameters passed in \code{...} as query
  parameters and then executes the query. If \code{result.colspec}
  specifies more that one column then tthe result is a data
  frame. Otherwise the result is the resulting column or in case of raw
  result (single raw value) a raw vector.
}
\value{
  \code{cl.prepare} returns a reference to the prepared query - it is of
  the class "casslitePreparedQuery".

  \code{cl.execute} returns the query result of \code{TRUE} if no result
  was requested.
}
\author{
  Simon Urbanek
}
\examples{
\dontrun{
## simple inster query
iq <- cl.prepare("INSERT INTO test (key, v) VALUES(?,?)")
cl.execute(iq, 0, "foo", "bar")
q <- cl.prepare("SELECT key, v FROM test")
cl.execute(q, 9L)
q <- cl.prepare("SELECT v FROM test WHERE key=?")
cl.execute(q,,"foo")

## insert a binary payload
cl.execute(iq, 0, "bin", serialize(1L, NULL))
## force raw result (useful for retrieveing binary data)
unserialize(cl.execute(q,-1,"bin"))

}
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }% use one of  RShowDoc("KEYWORDS")
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
