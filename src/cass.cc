#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "cassandra.h"

#include <Rinternals.h>

typedef struct tquery {
    CassSession *session;
    CassPrepared *prepared;
} tquery_t;

static void fin_tquery(SEXP ref) {
    tquery_t *q = (tquery_t*) R_ExternalPtrAddr(ref);
    if (q) {
	if (q->prepared) {
	    cass_prepared_free(q->prepared);
	    q->prepared = 0;
	}
	if (q->session) {
	    cass_session_free(q->session);
	    q->session = 0;
	}
	free(q);
    }
}

extern "C" SEXP OC_query_prepare(SEXP sWhere, SEXP sQuery) {
    CassCluster* cluster = cass_cluster_new();
 
    /* Contact points can be added as a comma-delimited list */
    cass_cluster_set_contact_points(cluster, CHAR(STRING_ELT(sWhere, 0)));

    CassSession* session = cass_session_new();

    CassFuture* connect_future = cass_session_connect(session, cluster);
 
    /* This operation will block until the result is ready */
    CassError rc = cass_future_error_code(connect_future);

    if (rc) Rf_error("Cannot connect to cluster '%s': %s",
		     CHAR(STRING_ELT(sWhere, 0)),
		     cass_error_desc(rc));

    cass_future_free(connect_future);    

    const char* insert_query = CHAR(STRING_ELT(sQuery, 0));
 
    /* Prepare the statement on the Cassandra cluster */
    CassFuture* prepare_future = cass_session_prepare(session, insert_query);
 
    /* Wait for the statement to prepare and get the result */
    rc = cass_future_error_code(prepare_future);
    
    if (rc) {
	cass_future_free(prepare_future);
	Rf_error("Failed to prepare statement: %s",
		 cass_error_desc(rc));
    }

    /* Get the prepared object from the future */
    const CassPrepared* prepared = cass_future_get_prepared(prepare_future);
 
    /* The future can be freed immediately after getting the prepared object */
    cass_future_free(prepare_future);

    tquery_t *q = (tquery_t*) malloc(sizeof(tquery_t));
    if (!q) {
	cass_prepared_free(prepared);
	cass_session_free(session);
	Rf_error("cannon allocate memory for the query object");
    }
    q->prepared = (CassPrepared*) prepared;
    q->session = session;
    
    SEXP res = PROTECT(R_MakeExternalPtr(q, R_NilValue, R_NilValue));
    R_RegisterCFinalizerEx(res, fin_tquery, TRUE);
    setAttrib(res, R_ClassSymbol, mkString("casslitePreparedQuery"));
    UNPROTECT(1);
    return res;
}

typedef struct tquery_future {
    CassFuture *future;
    int ncol;
} tquery_future_t;

static SEXP query_collect(tquery_future_t *q) {
    CassFuture *query_future = q->future;
    int cols = q->ncol;
    if (!query_future)
	Rf_error("the result has been already collected");

    /* This will block until the query has finished */
    CassError rc = cass_future_error_code(query_future);

    if (rc) {
	cass_future_free(query_future);
	q->future = 0;
	Rf_error("Failed insert statement: %s", cass_error_desc(rc));
    }

    if (cols == 0) { /* no result required */
	cass_future_free(query_future);
	q->future = 0;
	return ScalarLogical(1);
    }

    /* This will also block until the query returns */
    const CassResult* result = cass_future_get_result(query_future);

    /* If there was an error then the result won't be available */
    if (result == NULL) {
	/* Handle error */
	cass_future_free(query_future);
	q->future = 0;
	Rf_error("Failed select statement: %s",
		 cass_error_desc(rc));
    }

    /* The future can be freed immediately after getting the result object */
    cass_future_free(query_future);
    q->future = 0;

    size_t nrows = cass_result_row_count(result);

    /* exactly one row and raw result expected -> fetch the single value */
    if (cols == -1) {
	SEXP sRR = R_NilValue;
	if (nrows > 0) {
	    const CassRow* row = cass_result_first_row(result);
	    if (row) {
		const cass_byte_t *rv;
		size_t len = 0;
		cass_value_get_bytes(cass_row_get_column(row, 0), &rv, &len);
		sRR = allocVector(RAWSXP, len);
		memcpy(RAW(sRR), rv, len);
	    }
	    if (nrows > 1)
		Rf_warning("total %d rows, but returning only first row since raw result was desired", nrows);
	}
	cass_result_free(result);
	return sRR;
    }
    
    int ncol = 0, x = cols, ci = 0;
    while (x) { if (x & 1) ncol++; x >>= 1; }
    SEXP sRes  = PROTECT(allocVector(VECSXP, ncol));
    SEXP sId   = R_NilValue;
    if (cols & 1) { sId = SET_VECTOR_ELT(sRes, ci, allocVector(STRSXP, nrows)); ci++; }
    SEXP sTS   = R_NilValue;
    if (cols & 2) { sTS = SET_VECTOR_ELT(sRes, ci, allocVector(REALSXP, nrows)); ci++; }
    SEXP sAttr = R_NilValue;
    if (cols & 4) { sAttr = SET_VECTOR_ELT(sRes, ci, allocVector(STRSXP, nrows)); ci++; }
    SEXP sVal  = R_NilValue;
    if (cols & 8) { sVal = SET_VECTOR_ELT(sRes, ci, allocVector(STRSXP, nrows)); ci++; }
    double *ts_d = (sTS != R_NilValue) ? REAL(sTS) : 0;
    
    /* Create a new row iterator from the result */
    CassIterator* row_iterator = cass_iterator_from_result(result);

    R_xlen_t i = 0;
    while (cass_iterator_next(row_iterator)) {
	const CassRow* row = cass_iterator_get_row(row_iterator);
	const char *str = 0;
	cass_int64_t ts = 0;
	size_t len = 0;
	ci = 0;
	/* Copy data from the row */
	if (cols & 1) {
	    cass_value_get_string(cass_row_get_column(row, ci), &str, &len);
	    SET_STRING_ELT(sId,   i, mkCharLenCE(str, len, CE_UTF8));
	    ci++;
	}
	if (cols & 2) {
	    cass_value_get_int64 (cass_row_get_column(row, ci), &ts);
	    ts_d[i] = ((double) ts) / 1000.0;
	    ci++;
	}
	if (cols & 4) {
	    cass_value_get_string(cass_row_get_column(row, ci), &str, &len);
	    SET_STRING_ELT(sAttr, i, mkCharLenCE(str, len, CE_UTF8));
	    ci++;
	}
	if (cols & 8) {
	    cass_value_get_string(cass_row_get_column(row, ci), &str, &len);
	    SET_STRING_ELT(sVal,  i, mkCharLenCE(str, len, CE_UTF8));
	    ci++;
	}
	i++;
    }

    cass_iterator_free(row_iterator);

    /* don't bother with DF for a single result */
    if (ncol == 1) {
	UNPROTECT(1);
	return VECTOR_ELT(sRes, 0);
    }
    
    SEXP sRN = PROTECT(allocVector(INTSXP, 2));
    INTEGER(sRN)[0] = NA_INTEGER;
    INTEGER(sRN)[1] = -nrows;
    Rf_setAttrib(sRes, R_ClassSymbol, mkString("data.frame"));
    Rf_setAttrib(sRes, R_RowNamesSymbol, sRN);
    UNPROTECT(2);
    return sRes;
}

/* query, columns(integer bitmask), ... */
static CassFuture *OC_query_exec_(SEXP sPar, int *cols) {
    sPar = CDR(sPar);
    SEXP sQuery = CAR(sPar);
    sPar = CDR(sPar);
    if (!inherits(sQuery, "casslitePreparedQuery"))
	Rf_error("invalid prepared query object");

    tquery_t *q = (tquery_t*) R_ExternalPtrAddr(sQuery);

    cols[0] = asInteger(CAR(sPar));
    sPar = CDR(sPar);

    /* The prepared object can now be used to create statements that can be executed */
    CassStatement* statement = cass_prepared_bind(q->prepared);

    SEXP sVal;
    int pos = 0;
    while ((sVal = CAR(sPar)) != R_NilValue) {
	if (TYPEOF(sVal) == STRSXP)
	    cass_statement_bind_string(statement, pos, CHAR(STRING_ELT(sVal, 0)));
	else if (TYPEOF(sVal) == RAWSXP)
	    cass_statement_bind_bytes(statement, pos, RAW(sVal), XLENGTH(sVal));
	else if (TYPEOF(sVal) == INTSXP)
	    cass_statement_bind_int32(statement, pos, asInteger(sVal));
	else if (TYPEOF(sVal) == REALSXP)
	    cass_statement_bind_double(statement, pos, asReal(sVal));
	else
	    cass_statement_bind_null(statement, pos);
	pos++;
	sPar = CDR(sPar);
    }

    /* Execute statement (same a the non-prepared code) */
    CassFuture* query_future = cass_session_execute(q->session, statement);

    /* Statement objects can be freed immediately after being executed */
    cass_statement_free(statement);

    return query_future;
}

extern "C" SEXP OC_query_exec(SEXP sPar) {
    tquery_future_t q = { 0, 0 };
    q.future = OC_query_exec_(sPar, &q.ncol);
    return query_collect(&q);
}

static void fin_tquery_future(SEXP ref) {
    tquery_future_t *q = (tquery_future_t*) R_ExternalPtrAddr(ref);
    if (q) {
	if (q->future) {
	    cass_future_free(q->future);
	    q->future = 0;
	}
	free(q);
    }
}

extern "C" SEXP OC_query_exec_async(SEXP sPar) {
    tquery_future_t *qf = (tquery_future_t*) malloc(sizeof(tquery_future_t));
    if (!qf)
	Rf_error("unable to allocate memory for query future on the R side");
    qf->future = OC_query_exec_(sPar, &(qf->ncol));

    SEXP res = PROTECT(R_MakeExternalPtr(qf, R_NilValue, R_NilValue));
    R_RegisterCFinalizerEx(res, fin_tquery_future, TRUE);
    setAttrib(res, R_ClassSymbol, mkString("cassliteQueryFuture"));
    UNPROTECT(1);

    return res;
}

extern "C" SEXP OC_query_is_ready(SEXP sQuery) {
    if (!inherits(sQuery, "cassliteQueryFuture") || TYPEOF(sQuery) != EXTPTRSXP)
	Rf_error("invalid query future object");
    tquery_future_t *q = (tquery_future_t*) R_ExternalPtrAddr(sQuery);
    if (!q->future)
	Rf_error("the result has been already retrieved");
    return ScalarLogical(cass_future_ready(q->future) ? 1 : 0);
}

extern "C" SEXP OC_query_collect(SEXP sQuery) {
    if (!inherits(sQuery, "cassliteQueryFuture") || TYPEOF(sQuery) != EXTPTRSXP)
	Rf_error("invalid query future object");
    tquery_future_t *q = (tquery_future_t*) R_ExternalPtrAddr(sQuery);
    if (!q->future)
	Rf_error("the result has been already retrieved");
    SEXP res = query_collect(q);
    q->future = 0;
    return res;
}
