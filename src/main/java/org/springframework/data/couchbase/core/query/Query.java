/*
 * Copyright 2012-2020 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.core.query;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.kv.MutationState;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.manager.collection.ScopeSpec;
import com.couchbase.client.java.query.QueryProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.support.CollectionName;
import org.springframework.data.couchbase.core.support.ScopeName;
import org.springframework.data.couchbase.repository.query.StringBasedCouchbaseQuery;
import org.springframework.data.couchbase.repository.query.StringBasedN1qlQueryParser;
import org.springframework.data.couchbase.repository.support.MappingCouchbaseEntityInformation;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.Alias;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.json.JsonValue;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;

/**
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
public class Query {

	private final List<QueryCriteriaDefinition> criteria = new ArrayList<>();
	private JsonValue parameters = JsonValue.ja();
	private long skip;
	private int limit;
	private Sort sort = Sort.unsorted();
	private QueryScanConsistency queryScanConsistency;
	private CommonOptions<?> couchbaseOptions;
	private CollectionSpec collection;
	private ScopeSpec scope;
	private CollectionName collectionName;
	private ScopeName scopeName;

	static private final Pattern WHERE_PATTERN = Pattern.compile("\\sWHERE\\s");
	private static final Logger LOG = LoggerFactory.getLogger(Query.class);

	public Query() {}

	public Query(final QueryCriteriaDefinition criteriaDefinition) {
		addCriteria(criteriaDefinition);
	}

	public static Query query(QueryCriteriaDefinition criteriaDefinition) {
		return new Query(criteriaDefinition);
	}

	public Query addCriteria(QueryCriteriaDefinition criteriaDefinition) {
		this.criteria.add(criteriaDefinition);
		return this;
	}

	/**
	 * set the postional parameters on the query object There can only be named parameters or positional parameters - not
	 * both.
	 *
	 * @param parameters - the positional parameters
	 * @return - the query
	 */
	public Query setPositionalParameters(JsonArray parameters) {
		this.parameters = parameters;
		return this;
	}

	/**
	 * set the named parameters on the query object There can only be named parameters or positional parameters - not
	 * both.
	 *
	 * @param parameters - the named parameters
	 * @return - the query
	 */
	public Query setNamedParameters(JsonObject parameters) {
		this.parameters = parameters;
		return this;
	}

	JsonValue getParameters() {
		return parameters;
	}

	/**
	 * Set number of documents to skip before returning results.
	 *
	 * @param skip
	 * @return
	 */
	public Query skip(long skip) {
		this.skip = skip;
		return this;
	}

	/**
	 * Limit the number of returned documents to {@code limit}.
	 *
	 * @param limit
	 * @return
	 */
	public Query limit(int limit) {
		this.limit = limit;
		return this;
	}

	/**
	 * Sets the given pagination information on the {@link Query} instance. Will transparently set {@code skip} and
	 * {@code limit} as well as applying the {@link Sort} instance defined with the {@link Pageable}.
	 *
	 * @param pageable
	 * @return
	 */
	public Query with(final Pageable pageable) {
		if (pageable.isUnpaged()) {
			return this;
		}
		this.limit = pageable.getPageSize();
		this.skip = pageable.getOffset();
		return with(pageable.getSort());
	}

	/**
	 * queryScanConsistency
	 *
	 * @return queryScanConsistency
	 */
	public QueryScanConsistency getScanConsistency() {
		return queryScanConsistency;
	}

	/**
	 * Sets the given scan consistency on the {@link Query} instance.
	 *
	 * @param queryScanConsistency
	 * @return this
	 */
	public Query scanConsistency(final QueryScanConsistency queryScanConsistency) {
		this.queryScanConsistency = queryScanConsistency;
		return this;
	}

	/**
	 * Adds a {@link Sort} to the {@link Query} instance.
	 *
	 * @param sort
	 * @return
	 */
	public Query with(final Sort sort) {
		Assert.notNull(sort, "Sort must not be null!");
		if (sort.isUnsorted()) {
			return this;
		}
		this.sort = this.sort.and(sort);
		return this;
	}

	public void appendSkipAndLimit(final StringBuilder sb) {
		if (limit > 0) {
			sb.append(" LIMIT ").append(limit);
		}
		if (skip > 0) {
			sb.append(" OFFSET ").append(skip);
		}
	}

	public void appendSort(final StringBuilder sb) {
		if (sort.isUnsorted()) {
			return;
		}

		sb.append(" ORDER BY ");
		sort.stream().forEach(order -> {
			if (order.isIgnoreCase()) {
				throw new IllegalArgumentException(String.format("Given sort contained an Order for %s with ignore case! "
						+ "Couchbase N1QL does not support sorting ignoring case currently!", order.getProperty()));
			}
			sb.append(order.getProperty()).append(" ").append(order.isAscending() ? "ASC," : "DESC,");
		});
		sb.deleteCharAt(sb.length() - 1);
	}

	public void appendWhere(final StringBuilder sb, int[] paramIndexPtr, CouchbaseConverter converter) {
		if (!criteria.isEmpty()) {
			appendWhereOrAnd(sb);
			boolean first = true;
			for (QueryCriteriaDefinition c : criteria) {
				if (first) {
					first = false;
				} else {
					sb.append(" AND ");
				}
				sb.append(c.export(paramIndexPtr, parameters, converter));
			}
		}
	}

	public void appendWhereString(StringBuilder sb, String whereString) {
		appendWhereOrAnd(sb);
		sb.append(whereString);
	}

	public void appendString(StringBuilder sb, String whereString) {
		sb.append(whereString);
	}

	private void appendWhereOrAnd(StringBuilder sb) {
		String querySoFar = sb.toString().toUpperCase();
		Matcher whereMatcher = WHERE_PATTERN.matcher(querySoFar);
		boolean alreadyWhere = false;
		while (!alreadyWhere && whereMatcher.find()) {
			if (notQuoted(whereMatcher.start(), whereMatcher.end(), querySoFar)) {
				alreadyWhere = true;
			}
		}
		if (alreadyWhere) {
			sb.append(" AND ");
		} else {
			sb.append(" WHERE ");
		}
	}

	/**
	 * ensure that the WHERE we found was not quoted
	 *
	 * @param start
	 * @param end
	 * @param querySoFar
	 * @return true -> not quoted, false -> quoted
	 */
	private static boolean notQuoted(int start, int end, String querySoFar) {
		Matcher quoteMatcher = StringBasedN1qlQueryParser.QUOTE_DETECTION_PATTERN.matcher(querySoFar);
		List<int[]> quotes = new ArrayList<int[]>();
		while (quoteMatcher.find()) {
			quotes.add(new int[] { quoteMatcher.start(), quoteMatcher.end() });
		}

		for (int[] quote : quotes) {
			if (quote[0] <= start && quote[1] >= end) {
				return false; // it is quoted
			}
		}
		return true; // is not quoted
	}

	public String export(int[]... paramIndexPtrHolder) { // used only by tests
		StringBuilder sb = new StringBuilder();
		appendWhere(sb, paramIndexPtrHolder.length > 0 ? paramIndexPtrHolder[0] : null, null);
		appendSort(sb);
		appendSkipAndLimit(sb);
		return sb.toString();
	}

	public String toN1qlSelectString(ReactiveCouchbaseTemplate template, Class domainClass, boolean isCount) {
		return toN1qlSelectString(template, null, domainClass, null, isCount, null);
	}

	public String toN1qlSelectString(ReactiveCouchbaseTemplate template, String collectionName, Class domainClass,
			Class returnClass, boolean isCount, String[] distinctFields) {
		StringBasedN1qlQueryParser.N1qlSpelValues n1ql = getN1qlSpelValues(template, collectionName, domainClass,
				returnClass, isCount, distinctFields);
		final StringBuilder statement = new StringBuilder();
		appendString(statement, n1ql.selectEntity); // select ...
		appendWhereString(statement, n1ql.filter); // typeKey = typeValue
		appendWhere(statement, new int[] { 0 }, template.getConverter()); // criteria on this Query
		appendSort(statement);
		appendSkipAndLimit(statement);
		return statement.toString();
	}

	public String toN1qlRemoveString(ReactiveCouchbaseTemplate template, String collectionName, Class domainClass) {
		StringBasedN1qlQueryParser.N1qlSpelValues n1ql = getN1qlSpelValues(template, collectionName, domainClass, null,
				false, null);
		final StringBuilder statement = new StringBuilder();
		appendString(statement, n1ql.delete); // delete ...
		appendWhereString(statement, n1ql.filter); // typeKey = typeValue
		appendWhere(statement, null, template.getConverter()); // criteria on this Query
		appendString(statement, n1ql.returning);
		return statement.toString();
	}

	StringBasedN1qlQueryParser.N1qlSpelValues getN1qlSpelValues(ReactiveCouchbaseTemplate template, String collectionName,
			Class domainClass, Class returnClass, boolean isCount, String[] distinctFields) {
		String typeKey = template.getConverter().getTypeKey();
		final CouchbasePersistentEntity<?> persistentEntity = template.getConverter().getMappingContext()
				.getRequiredPersistentEntity(domainClass);
		MappingCouchbaseEntityInformation<?, Object> info = new MappingCouchbaseEntityInformation<>(persistentEntity);
		String typeValue = info.getJavaType().getName();
		TypeInformation<?> typeInfo = ClassTypeInformation.from(info.getJavaType());
		Alias alias = template.getConverter().getTypeAlias(typeInfo);
		if (alias != null && alias.isPresent()) {
			typeValue = alias.toString();
		}

		StringBasedN1qlQueryParser sbnqp = new StringBasedN1qlQueryParser(template.getBucketName(), collectionName,
				template.getConverter(), domainClass, returnClass, typeKey, typeValue, distinctFields);
		return isCount ? sbnqp.getCountContext() : sbnqp.getStatementContext();
	}

	/**
	 * build QueryOptions from parameters and scanConsistency
	 *
	 * @param scanConsistency
	 * @return QueryOptions
	 */
	public QueryOptions buildQueryOptions(QueryScanConsistency scanConsistency) {
		final QueryOptions options = QueryOptions.queryOptions();
		if (getParameters() != null) {
			if (getParameters() instanceof JsonArray) {
				options.parameters((JsonArray) getParameters());
			} else {
				options.parameters((JsonObject) getParameters());
			}
		}
		if (scanConsistency == null) {
			if (getScanConsistency() != null) {
				scanConsistency = getScanConsistency();
			}
		}
		if (scanConsistency != null) {
			options.scanConsistency(scanConsistency);
		}

		return mergeOptions(options);
	}

	private QueryOptions mergeOptions(QueryOptions requestOptions) {
		QueryOptions optionsParam = (QueryOptions) getCouchbaseOptions();
		if (optionsParam == null) {
			return requestOptions;
		}
		if (requestOptions == null) {
			return optionsParam;
		}

		JsonObject requestOpts = JsonObject.create();
		requestOptions.build().injectParams(requestOpts);
		LOG.debug("options before: {}", requestOpts);

		QueryOptions queryOptions = (QueryOptions) optionsParam;
		JsonObject queryOpts = JsonObject.create();
		QueryOptions.Built built = queryOptions.build();
		built.injectParams(queryOpts);
		LOG.debug("options merge : {}", queryOpts);

		// CommonOptions
		if (built.timeout() != null && built.timeout().isPresent()) {
			requestOptions.timeout(built.timeout().get());
		}
		if (built.retryStrategy() != null && built.retryStrategy().isPresent()) {
			requestOptions.retryStrategy(built.retryStrategy().get());
		}
		if (built.clientContext() != null) {
			requestOptions.clientContext(built.clientContext());
		}
		if (built.parentSpan() != null && built.parentSpan().isPresent()) {
			requestOptions.parentSpan(built.parentSpan().get());
		}

		// QueryOptions not injected
		if (built.serializer() != null) {
			requestOptions.serializer(built.serializer());
		}
		if (built.clientContext() != null) {
			requestOptions.clientContext(built.clientContext());
		}

		// everything that is injected
		// would it be better to just "raw" everything?
		for (String name : queryOpts.getNames()) {

			if (name.equals("adhoc")) {// private boolean adhoc = true; not accessible, not injected
				requestOptions.adhoc(queryOpts.getBoolean(name));
			} else if (name.equals("client_context_id")) {
				requestOptions.clientContextId(queryOpts.getString(name));
			} else if (name.equals("scan_vectors")) {
				requestOptions.consistentWith(MutationState.from(queryOpts.getObject(name).toString()));
				requestOptions.raw("scan_consistency", "at_plus");
				requestOpts = JsonObject.create(); // for check to not overwrite with queryOpts.getString("scan_consistency")
				requestOptions.build().injectParams(requestOpts);
			} else if (name.equals("max_parallelism")) {
				requestOptions.maxParallelism(queryOpts.getInt(name));
			} else if (name.equals("metrics")) {
				requestOptions.metrics(queryOpts.getBoolean(name));
			} else if (name.equals("args")) {
				requestOptions.parameters(queryOpts.getArray(name));
			} else if (name.equals("pipeline_batch")) {
				requestOptions.pipelineBatch(queryOpts.getInt(name));
			} else if (name.equals("profile")) {
				requestOptions.profile(QueryProfile.valueOf(queryOpts.getString(name).toUpperCase(Locale.ROOT)));
			} else if (name.equals("readonly")) {
				requestOptions.readonly(queryOpts.getBoolean(name));
			} else if (name.equals("scan_wait")) {
				requestOptions.scanWait(Duration.parse(queryOpts.getString(name)));
			} else if (name.equals("scan_cap")) {
				requestOptions.scanCap(queryOpts.getInt(name));
			} else if (name.equals("scan_consistency")) {
				// this must not overwrite "at_plus"
				if (!"at_plus".equals(requestOpts.getString(name))) {
					requestOptions
							.scanConsistency(QueryScanConsistency.valueOf(queryOpts.getString(name).toUpperCase(Locale.ROOT)));
				}
			} else if (name.equals("use_fts")) {
				requestOptions.flexIndex(queryOpts.getBoolean(name));
			} else {
				if (name.startsWith("$") && requestOpts.get("args") != null) {
					throw new IllegalArgumentException("cannot have both positional and named args. requestOpts: args:"
							+ requestOpts.get("args") + " paramOpts: " + name + ":" + queryOpts.get(name));
				}
				requestOptions.raw(name, queryOpts.get(name));
			}
		}

		if (LOG.isDebugEnabled()) {
			requestOpts = JsonObject.create();
			requestOptions.build().injectParams(requestOpts);
			LOG.debug("options after : {}", requestOpts);
		}
		return requestOptions;
	}

	/**
	 * if there is an additional arg collection such as  save(entity, CollectionSpec) or
	 * repository.withCollection(collectionName), use that
	 * otherwise use the collection based on the fluent inCollection(collectionName)
	 * this needs to be done before the caller uses the collection to decide how to make the call
	 *
	 * @param fluentCollection
	 * @return
	 */
	public CollectionSpec mergeCollectionSpec(CollectionSpec fluentCollection) {
		LOG.debug("collection before: {}", fluentCollection); // from fluent api
		LOG.debug("collection merge : {}", getCollection());   // from additional arg
		CollectionSpec resultCollection;
		if (getCollection() != null) {
			resultCollection = getCollection();
		} else {
			resultCollection = fluentCollection;
		}
		LOG.debug("collection after : {}", getCollection());
		if( resultCollection != null && getScope() != null){
			if ( !getScope().collections().contains(resultCollection)){
				throw new IllegalArgumentException("scope specified does not contain collection specified: scope: "+getScope()+" collection: "+getCollection());
			}
		}

		return resultCollection;
	}

	/**
	 * if there is an additional arg scope such as  save(entity, ScopeSpec) or
	 * repository.withScope(scopeName|scopeSpec), use that
	 * otherwise use the scope based on the fluent inScope(scopeName)
	 * this needs to be done before the caller uses the collection to decide how to make the call
	 *
	 * @param fluentScope
	 * @return
	 */
	public ScopeSpec mergeScopeSpec(ScopeSpec fluentScope) {
		LOG.debug("scope before: {}", fluentScope); // from fluent api
		LOG.debug("scope merge : {}", getScope());  // from additional arg

		ScopeSpec resultScope=null;

		if (getScope() == null) {
			resultScope = fluentScope;
		} else if (fluentScope == null || ! fluentScope.name().equals(getScope().name())){
			resultScope =  getScope();
		} else { // create new scope from additional arg scope plus collections from fluent scope
			Set<CollectionSpec> collections = fluentScope.collections();
			collections.addAll(getScope().collections());
			resultScope = ScopeSpec.create(getScope().name(), collections);
		}
		LOG.debug("scope after : {}", getScope());
		if( getCollection() != null && resultScope != null && resultScope.collections() != null ) { //TODO:
			if ( !resultScope.collections().contains(getCollection())){
				resultScope.collections().add(getCollection());
				//throw new IllegalArgumentException("scope specified does not contain collection specified: scope: "+resultScope+" collection: "+getCollection());
			}
		} else if ( getCollection() == null){
			if( resultScope != null){
				if ( resultScope.collections() != null){
					if ( resultScope.collections().size() != 1){
						throw new IllegalArgumentException("no collection specified, and scope contains zero or multiple collections "+getScope());
					}
					// setCollection(getScope().collections().iterator().next());
				}
			}
		}
		return resultScope;
	}

	public void setMeta(Meta metaAnnotation) {
		Meta meta = metaAnnotation;
	}

	public void setCouchbaseOptions(CommonOptions<?> couchbaseOptions) {
		this.couchbaseOptions = couchbaseOptions;
	}

	public CommonOptions<?> getCouchbaseOptions() {
		return this.couchbaseOptions;
	}

	public void setCollection(CollectionSpec collection) {
		this.collection = collection;
	}

	public CollectionSpec getCollection() {
		return this.collection;
	}

	public void setScope(ScopeSpec scope) {
		this.scope = scope;
	}

	public ScopeSpec getScope() {
		return this.scope;
	}

	public void setCollectionName(CollectionName collectionName) {
		this.collectionName = collectionName;
	}

	public CollectionName getCollectionName() {
		return this.collectionName;
	}

	public void setScopeName(ScopeName scopeName) {
		this.scopeName = scopeName;
	}

	public ScopeName getScopeName() {
		return this.scopeName;
	}
}
