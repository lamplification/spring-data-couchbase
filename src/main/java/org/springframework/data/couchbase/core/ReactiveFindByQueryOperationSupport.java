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
package org.springframework.data.couchbase.core;

import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.manager.collection.ScopeSpec;
import com.couchbase.client.java.query.QueryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.support.TemplateUtils;
import org.springframework.util.Assert;

import com.couchbase.client.java.query.QueryScanConsistency;
import com.couchbase.client.java.query.ReactiveQueryResult;

/**
 * {@link ReactiveFindByQueryOperation} implementations for Couchbase.
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
public class ReactiveFindByQueryOperationSupport implements ReactiveFindByQueryOperation {

	private static final Query ALL_QUERY = new Query();

	private final ReactiveCouchbaseTemplate template;

	private static final Logger LOG = LoggerFactory.getLogger(ReactiveFindByQueryOperationSupport.class);

	public ReactiveFindByQueryOperationSupport(final ReactiveCouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ReactiveFindByQuery<T> findByQuery(final Class<T> domainType) {
		return new ReactiveFindByQuerySupport<>(template, domainType, domainType, ALL_QUERY,
				QueryScanConsistency.NOT_BOUNDED, null, null);
	}

	static class ReactiveFindByQuerySupport<T> implements ReactiveFindByQuery<T> {

		private final ReactiveCouchbaseTemplate template;
		private final Class<?> domainType;
		private final Class<T> returnType;
		private final Query query;
		private final QueryScanConsistency scanConsistency;
		private final String collectionName;
		// scopeName is not final so it can be overridden with query.getScopeName()
		private String scopeName; // unused - there is no withScope(scopeName) on the template fluent api (yet).
		private final String[] distinctFields;
		// this would hold scanConsistency etc. from the fluent api if they were converted from standalone fields
		// withScope(scopeName) could put raw("query_context",default:<bucket>.<scope>)
		// this is not the options argument in save( entity, options ). That becomes query.getCouchbaseOptions()
		private final QueryOptions options = QueryOptions.queryOptions();
		// this would hold collectionSpec from the fluent api
		// private CollectionSpec collectionSpec;

		ReactiveFindByQuerySupport(final ReactiveCouchbaseTemplate template, final Class<?> domainType,
				final Class<T> returnType, final Query query, final QueryScanConsistency scanConsistency,
				final String collection, final String[] distinctFields) {
			Assert.notNull(domainType, "domainType must not be null!");
			Assert.notNull(returnType, "returnType must not be null!");

			this.template = template;
			this.domainType = domainType;
			this.returnType = returnType;
			this.query = query;
			this.scanConsistency = scanConsistency;
			this.collectionName = collection;
			this.scopeName = null;
			this.distinctFields = distinctFields;
		}

		@Override
		public FindByQueryWithQuery<T> matching(Query query) {
			QueryScanConsistency scanCons;
			if (query.getScanConsistency() != null) {
				scanCons = query.getScanConsistency();
			} else {
				scanCons = scanConsistency;
			}
			return new ReactiveFindByQuerySupport<>(template, domainType, returnType, query, scanCons,
					collectionName,
					distinctFields);
		}

		@Override
		public FindByQueryInCollection<T> inCollection(String collection) {
			Assert.hasText(collection, "Collection must not be null nor empty.");
			return new ReactiveFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency, collection,
					distinctFields);
		}

		@Override
		@Deprecated
		public FindByQueryConsistentWith<T> consistentWith(QueryScanConsistency scanConsistency) {
			return new ReactiveFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency,
					collectionName,
					distinctFields);
		}

		@Override
		public FindByQueryWithConsistency<T> withConsistency(QueryScanConsistency scanConsistency) {
			return new ReactiveFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency,
					collectionName,
					distinctFields);
		}

		@Override
		public <R> FindByQueryWithConsistency<R> as(Class<R> returnType) {
			Assert.notNull(returnType, "returnType must not be null!");
			return new ReactiveFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency,
					collectionName,
					distinctFields);
		}

		@Override
		public FindByQueryWithDistinct<T> distinct(String[] distinctFields) {
			Assert.notNull(distinctFields, "distinctFields must not be null!");
			return new ReactiveFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency,
					collectionName,
					distinctFields);
		}

		@Override
		public Mono<T> one() {
			return all().singleOrEmpty();
		}

		@Override
		public Mono<T> first() {
			return all().next();
		}

		@Override
		public Flux<T> all() {
			return Flux.defer(() -> {
				LOG.info("query: {} query.getCollection(): {}", query, query.getCollection());
				// If there is no collection and no scope, or _default._default
				// for ById, just collection name from collection is used
				// for Query, both the Scope and Collection name from collection need to be used
				// if scope name is _use_clientFactory_scope or null, use scope from clientFactory

				// There is (currently) no scope from fluent api (withScope(scopeName)), but if there were, this is how we would
				// create the ScopeSpec
				// It would just have the scopeName, no collections.
				/****************************************
				ScopeSpec fluentScope = scopeName != null ? ScopeSpec.create(scopeName, new HashSet<CollectionSpec>())
						: ScopeSpec.create(template.getCouchbaseClientFactory().getScope().name(), new HashSet<CollectionSpec>());

				// merge the fluent scope with scope from extra ScopeSpec arg
				// the extra ScopeSpec arg/repository.withScope() overrides the fluentScope
				ScopeSpec ss = query.mergeScopeSpec(fluentScope);

				// collectionName from fluent api
				CollectionSpec fluentCollection = collection != null
						? CollectionSpec.create(collection, template.getCouchbaseClientFactory().getScope().name())
						: null;
				// merge collection (override) with collection from extra CollectionSpec arg
				CollectionSpec cs = query.mergeCollectionSpec(fluentCollection);

				if (cs == null && ss != null)
					cs = ss.collections().iterator().next();

				String scopeName = ss != null ? ss.name() : (cs != null ? cs.scopeName() : null);
				String collectionName = cs != null ? cs.name()
						: (ss != null ? ss.collections().iterator().next().name() : null);

				if ("_use_clientFactory_scope".equals(scopeName)) { // this is from withCollection() of DynamicProxy
					scopeName = template.getCouchbaseClientFactory().getScope() == null ? null
							: template.getCouchbaseClientFactory().getScope().name();
				}
				if ("_default".equals(scopeName) && "_default".equals(collectionName)) {
					scopeName = null;
					collectionName = null;
				}
				 **********************************************************/
				// this.scopeName is from template fluent api
				// query.getScopeName is from ScopeName argument to repository call (better would be from q.getCouchbaseOptions())
				// the scopeName from options will override scopeName from fluent api.
				String scopeForQuery =  query.getScopeName() != null ? query.getScopeName().toString() : scopeName;
				String collectionForQuery =  query.getCollectionName() != null ? query.getCollectionName().toString() : collectionName;
				if( (scopeForQuery == null || "_default".equals(scopeForQuery)) && (collectionForQuery == null || "_default".equals(collectionForQuery))){
					scopeForQuery = null;
					collectionForQuery = null;
				}
				if(collectionForQuery != null && scopeForQuery == null ) {
					scopeForQuery = template.getCouchbaseClientFactory().getScope().name();
				}
				scopeName = scopeForQuery;
				String statement = assembleEntityQuery(false, distinctFields, collectionForQuery);

				LOG.info("statement: {}", statement);
				LOG.info("scopeName: {} factory.getScope(): {} query.getCollection(): {}", scopeForQuery,
						template.getCouchbaseClientFactory().getScope(), query.getCollection());
				// instead of using getCluster() or getScope(), we can just set options.raw(query_context, default:bucket.scope)
				Mono<ReactiveQueryResult> allResult = scopeForQuery == null
						? template.getCouchbaseClientFactory().getCluster().reactive().query(statement, buildOptions())
						: template.getCouchbaseClientFactory().withScope(scopeForQuery).getScope().reactive().query(statement,
								buildOptions());
				return allResult.onErrorMap(throwable -> {
					if (throwable instanceof RuntimeException) {
						return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
					} else {
						return throwable;
					}
				}).flatMapMany(ReactiveQueryResult::rowsAsObject).map(row -> {
					String id = "";
					long cas = 0;
					if (distinctFields == null) {
						id = row.getString(TemplateUtils.SELECT_ID);
						cas = row.getLong(TemplateUtils.SELECT_CAS);
						row.removeKey(TemplateUtils.SELECT_ID);
						row.removeKey(TemplateUtils.SELECT_CAS);
					}
					return template.support().decodeEntity(id, row.toString(), cas, returnType);
				});
			});
		}

		@Override
		public QueryOptions buildOptions() {
			QueryOptions opts = query.buildQueryOptions(scanConsistency);
			if( scopeName != null)
				opts.raw("query_context","default:`"+template.getCouchbaseClientFactory().getBucket().name()+"`."+scopeName);
			return opts;
		}

		@Override
		public Mono<Long> count() {
			return Mono.defer(() -> {
				CollectionSpec queryCollection = collectionName != null
						? CollectionSpec.create(collectionName, template.getCouchbaseClientFactory().getScope().name())
						: null;
				CollectionSpec cs = query.mergeCollectionSpec(queryCollection);
				ScopeSpec ss = query.mergeScopeSpec(null);
				String statement = assembleEntityQuery(true, distinctFields, cs == null ? null : cs.name());
				String scopeName = ss != null ? ss.name() : (cs != null ? cs.scopeName() : null);
				if (scopeName == "unused") {
					scopeName = template.getCouchbaseClientFactory().getScope() == null ? null
							: template.getCouchbaseClientFactory().getScope().name();
				}
				Mono<ReactiveQueryResult> countResult = this.collectionName == null
						? template.getCouchbaseClientFactory().getCluster().reactive().query(statement, buildOptions())
						: template.getCouchbaseClientFactory().withScope(scopeName).getScope().reactive().query(statement,
								buildOptions());
				return countResult.onErrorMap(throwable -> {
					if (throwable instanceof RuntimeException) {
						return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
					} else {
						return throwable;
					}
				}).flatMapMany(ReactiveQueryResult::rowsAsObject).map(row -> {
					return row.getLong(TemplateUtils.SELECT_COUNT);
				}).next();
			});
		}

		@Override
		public Mono<Boolean> exists() {
			return count().map(count -> count > 0); // not efficient, just need the first one
		}

		private String assembleEntityQuery(final boolean count, String[] distinctFields, String collection) {
			return query.toN1qlSelectString(template, collection, this.domainType, this.returnType, count, distinctFields);
		}
	}
}
