/*
 * Copyright 2013-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.repository.support;

// x Optional<T>  findById();
// x List<T> findAll();
// x List<T> findAll(Query);
// x List<T> findAll(Sort sort);
// x Page<T> findAll(Pageable pageable);
// x List<T> findAll(QueryScanConsistency queryScanConsistency);
// x List<T> findAllById(Iterable<ID> iterable);
// x boolean existsById(ID id);
// x long count();
// x <S extends T> S save(S entity); // UpsertOptions
// x <S extends T> S save(S entity); // ReplaceOptions
// x <S extends T> Iterable<S> saveAll(Iterable<S> entities); // UpsertOptions
// x <S extends T> Iterable<S> saveAll(Iterable<S> entities); // ReplaceOptions
// x void deleteById(ID id);
// x void delete(T entity);
// x void deleteAllById(Iterable<? extends ID> ids);
// x void deleteAll(Iterable<? extends T> entities);
// x void deleteAll();

import static org.springframework.data.couchbase.repository.support.Util.hasNonZeroVersionProperty;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.ReplaceOptions;
import com.couchbase.client.java.kv.UpsertOptions;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.manager.collection.ScopeSpec;
import com.couchbase.client.java.query.QueryOptions;
import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ExecutableRemoveByIdOperation;
import org.springframework.data.couchbase.core.ExecutableUpsertByIdOperation;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.support.CollectionName;
import org.springframework.data.couchbase.core.support.MapList;
import org.springframework.data.couchbase.core.support.ScopeName;
import org.springframework.data.couchbase.repository.CouchbaseRepository;
import org.springframework.data.couchbase.repository.query.CouchbaseEntityInformation;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.Repository;
import org.springframework.data.util.StreamUtils;
import org.springframework.data.util.Streamable;
import org.springframework.util.Assert;

import com.couchbase.client.java.query.QueryScanConsistency;

/**
 * Repository base implementation for Couchbase.
 *
 * @author Michael Nitschinger
 * @author Mark Paluch
 * @author Jens Schauder
 */
public class SimpleCouchbaseRepository<T, ID> implements CouchbaseRepository<T, ID> {

	/**
	 * Holds the reference to the {@link org.springframework.data.couchbase.core.CouchbaseTemplate}.
	 */
	private final CouchbaseOperations couchbaseOperations;

	/**
	 * Contains information about the entity being used in this repository.
	 */
	private final CouchbaseEntityInformation<T, String> entityInformation;

	private CrudMethodMetadata crudMethodMetadata;

	/**
	 * Create a new Repository.
	 *
	 * @param entityInformation the Metadata for the entity.
	 * @param couchbaseOperations the reference to the template used.
	 */
	public SimpleCouchbaseRepository(CouchbaseEntityInformation<T, String> entityInformation,
			CouchbaseOperations couchbaseOperations) {
		Assert.notNull(entityInformation, "CouchbaseEntityInformation must not be null!");
		Assert.notNull(couchbaseOperations, "CouchbaseOperations must not be null!");

		this.entityInformation = entityInformation;
		this.couchbaseOperations = couchbaseOperations;
		getEntityInformation();
	}

	// Optional<T> findById(ID id);

	@Override
	public Optional<T> findById(ID id) {
		return findById(id, null, null, null);
	}

	@Override
	public Optional<T> findById(ID id, QueryOptions... options) {
		return findById(id, get(options), null, null);
	}

	@Override
	public Optional<T> findById(ID id, QueryOptions options, CollectionName collectionName, ScopeName... scopeNames) {
		Assert.notNull(id, "The given id must not be null!");
		return Optional.ofNullable(couchbaseOperations.findById(entityInformation.getJavaType())
				.inCollection(get(collectionName)).one(id.toString()));
	}

	@Override
	public Optional<T> findById(ID id, CollectionName collectionName, ScopeName... scopeNames) {
		return findById(id, null, collectionName, scopeNames);
	}

	// List<T> findAll();
	@Override
	public List<T> findAll() {
		return findAll((QueryOptions) null, null, null);
	}

	@Override
	public List<T> findAll(QueryOptions... options) {
		return findAll(get(options), null, null);
	}

	/**
	 * Helper method to assemble a n1ql find all query, taking annotations into acocunt.
	 *
	 * @return the list of found entities, already executed.
	 */
	@Override
	public List<T> findAll(QueryOptions options, CollectionName collectionName, ScopeName... scopeName) {
		return couchbaseOperations.findByQuery(entityInformation.getJavaType()).withConsistency(buildQueryScanConsistency())
				.inCollection(get(collectionName)).all();
	}

	@Override
	public List<T> findAll(CollectionName collectionName, ScopeName... scopeNames) {
		return findAll((QueryOptions) null, collectionName, scopeNames);
	}

	@Override
	public List<T> findAll(Query query) {
		return findAll(query, null, null, null);
	}

	@Override
	public List<T> findAll(Query query, QueryOptions... options) {
		return findAll(query, get(options), null, null);
	}

	@Override
	public List<T> findAll(Query query, QueryOptions options, CollectionName collectionName, ScopeName... scopeName) {
		return couchbaseOperations.findByQuery(entityInformation.getJavaType()).withConsistency(buildQueryScanConsistency())
				.inCollection(get(collectionName)).matching(query).all();
	}

	@Override
	public List<T> findAll(Query query, CollectionName collectionName, ScopeName... scopeNames) {
		return findAll(query, null, collectionName, scopeNames);
	}

	// List<T> findAll(Sort sort);

	@Override
	public List<T> findAll(Sort sort) {
		return findAll(sort, null, null, null);
	}

	@Override
	public List<T> findAll(Sort sort, QueryOptions... options) {
		return findAll(sort, get(options), null, null);
	}

	@Override
	public List<T> findAll(Sort sort, QueryOptions options, CollectionName collectionName, ScopeName... scopeName) {
		return findAll(new Query().with(sort), options, collectionName, scopeName);
	}

	@Override
	public List<T> findAll(Sort sort, CollectionName collectionName, ScopeName... scopeNames) {
		return findAll(sort, null, collectionName, scopeNames);
	}

	// List<T> findAll(Pageable);

	@Override
	public Page<T> findAll(Pageable pageable) {
		return findAll(pageable, null, null, null);
	}

	@Override
	public Page<T> findAll(Pageable pageable, QueryOptions... options) {
		return findAll(pageable, get(options), null, null);
	}

	@Override
	public Page<T> findAll(Pageable pageable, QueryOptions options, CollectionName collectionName,
			ScopeName... scopeNames) {
		List<T> results = findAll(new Query().with(pageable), options, collectionName, scopeNames);
		return new PageImpl<>(results, pageable, count());
	}

	@Override
	public Page<T> findAll(Pageable pageable, CollectionName collectionName, ScopeName... scopeNames) {
		return findAll(pageable, null, collectionName, scopeNames);
	}

	// List<T> findAll(QueryScanConsistency);

	@Override
	public List<T> findAll(QueryScanConsistency queryScanConsistency) {
		return findAll(queryScanConsistency, null, null, null);
	}

	@Override
	public List<T> findAll(QueryScanConsistency queryScanConsistency, QueryOptions... options) {
		return findAll(queryScanConsistency, get(options), null, null);
	}

	@Override
	public List<T> findAll(QueryScanConsistency queryScanConsistency, QueryOptions options, CollectionName collectionName,
			ScopeName... scopeNames) {
		return findAll(new Query().scanConsistency(queryScanConsistency), options, collectionName, scopeNames);
	}

	@Override
	public List<T> findAll(QueryScanConsistency queryScanConsistency, CollectionName collectionName,
			ScopeName... scopeNames) {
		return findAll(queryScanConsistency, null, collectionName, scopeNames);
	}

	// List<T> findAllById(Iterable<ID> ids)

	@Override
	public List<T> findAllById(Iterable<ID> ids) {
		return findAllById(ids, null, null, null);
	}

	@Override
	public List<T> findAllById(Iterable<ID> ids, QueryOptions... options) {
		return findAllById(ids, get(options), null, null);
	}

	@Override
	public List<T> findAllById(Iterable<ID> ids, QueryOptions options, CollectionName collectionName,
			ScopeName... scopeNames) {
		Assert.notNull(ids, "The given Iterable of ids must not be null!");
		String collName = collectionName == null ? null : collectionName.name();
		List<String> convertedIds = Streamable.of(ids).stream().map(Objects::toString).collect(Collectors.toList());
		Collection<? extends T> all = couchbaseOperations.findById(entityInformation.getJavaType()).inCollection(collName)
				.all(convertedIds);
		return Streamable.of(all).stream().collect(StreamUtils.toUnmodifiableList());
	}

	@Override
	public List<T> findAllById(Iterable<ID> ids, CollectionName collectionName, ScopeName... scopeNames) {
		return findAllById(ids, null, collectionName, scopeNames);
	}

	// boolean existsById(ID id)

	@Override
	public boolean existsById(ID id) {
		return existsById(id, null, null, null);
	}

	@Override
	public boolean existsById(ID id, QueryOptions... options) {
		return false;
	}

	@Override
	public boolean existsById(ID id, QueryOptions options, CollectionName collectionName, ScopeName... scopeNames) {
		Assert.notNull(id, "The given id must not be null!");
		String collName = collectionName == null ? null : collectionName.name();
		return couchbaseOperations.existsById().inCollection(collName).one(id.toString());
	}

	@Override
	public boolean existsById(ID id, CollectionName collectionName, ScopeName... scopeNames) {
		return false;
	}

	// long count()

	@Override
	public long count() {
		return count(null, null, null);
	}

	@Override
	public long count(QueryOptions... options) {
		return count(get(options), null, null);
	}

	@Override
	public long count(QueryOptions options, CollectionName collectionName, ScopeName... scopeNames) {
		return couchbaseOperations.findByQuery(entityInformation.getJavaType()).withConsistency(buildQueryScanConsistency())
				.inCollection(get(collectionName)).count();
	}

	@Override
	public long count(CollectionName collectionName, ScopeName... scopeName) {
		return count(null, collectionName, scopeName);
	}

	// <S extends T> S save(S entity) UpsertOptions

	@Override
	public <S extends T> S save(S entity) {
		return save(entity, (UpsertOptions) null, null, null);
	}

	@Override
	public <S extends T> S save(S entity, UpsertOptions... options) {
		return save(entity, get(options), null, null);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <S extends T> S save(S entity, UpsertOptions options, CollectionName collectionName, ScopeName... scopeNames) {
		Assert.notNull(entity, "Entity must not be null!");
		// if entity has non-null, non-zero version property, then replace()
		if (hasNonZeroVersionProperty(entity, couchbaseOperations.getConverter())) {
			return (S) couchbaseOperations.replaceById(entityInformation.getJavaType()).inCollection(get(collectionName))
					.one(entity);
		} else {
			return (S) couchbaseOperations.upsertById(entityInformation.getJavaType()).inCollection(get(collectionName))
					.one(entity);
		}
	}

	@Override
	public <S extends T> S save(S entity, CollectionName collectionName, ScopeName... scopeNames) {
		return save(entity, (UpsertOptions) null, collectionName, scopeNames);
	}

	// <S extends T> S save(S entity) ReplaceOptions

	// public <S extends T> S save(S entity) { already defined - in save() UpsertOptions }

	@Override
	public <S extends T> S save(S entity, ReplaceOptions... options) {
		return save(entity, get(options), null, null);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <S extends T> S save(S entity, ReplaceOptions options, CollectionName collectionName,
			ScopeName... scopeNames) {
		Assert.notNull(entity, "Entity must not be null!");
		// if entity has non-null, non-zero version property, then replace()
		if (hasNonZeroVersionProperty(entity, couchbaseOperations.getConverter())) {
			return (S) couchbaseOperations.replaceById(entityInformation.getJavaType()).inCollection(get(collectionName))
					.one(entity);
		} else {
			return (S) couchbaseOperations.upsertById(entityInformation.getJavaType()).inCollection(get(collectionName))
					.one(entity);
		}
	}

	// public <S extends T> S save(S entity, CollectionName collectionName, ScopeName... scopeNames) // already defined

	// <S extends T> Iterable<S> saveAll(Iterable<S> entities) UpsertOptions
	@Override
	public <S extends T> Iterable<S> saveAll(Iterable<S> entities) {
		return saveAll(entities, (UpsertOptions) null, null, null);
	}

	@Override
	public <S extends T> Iterable<S> saveAll(Iterable<S> entities, UpsertOptions... options) {
		return saveAll(entities, get(options), null, null);
	}

	@Override
	public <S extends T> Iterable<S> saveAll(Iterable<S> entities, UpsertOptions options, CollectionName collectionName,
			ScopeName... scopeNames) {
		Assert.notNull(entities, "The given Iterable of entities must not be null!");
		return Streamable.of(entities).stream().map((e) -> save(e, options, collectionName, scopeNames))
				.collect(StreamUtils.toUnmodifiableList());
	}

	@Override
	public <S extends T> Iterable<S> saveAll(Iterable<S> entities, CollectionName collectionName,
			ScopeName... scopeName) {
		return null;
	}

	// <S extends T> Iterable<S> saveAll(Iterable<S> entities) UpsertOptions

	@Override
	public <S extends T> Iterable<S> saveAll(Iterable<S> entities, ReplaceOptions... options) {
		return saveAll(entities, get(options), null, null);
	}

	@Override
	public <S extends T> Iterable<S> saveAll(Iterable<S> entities, ReplaceOptions options, CollectionName collectionName,
			ScopeName... scopeNames) {
		return null;
	}

	// public <S extends T> S saveAll(Iterable<S> entities, CollectionName collectionName, ScopeName... scopeNames) //
	// already defined

	// void deleteById(ID id)

	@Override
	public void deleteById(ID id) {
		deleteById(id, null, null, null);
	}

	@Override
	public void deleteById(ID id, RemoveOptions... options) {
		deleteById(id, get(options), null, null);
	}

	@Override
	public void deleteById(ID id, RemoveOptions optins, CollectionName collectionName, ScopeName... scopeNames) {
		Assert.notNull(id, "The given id must not be null!");
		couchbaseOperations.removeById().inCollection(get(collectionName)).one(id.toString());
	}

	@Override
	public void deleteById(ID id, CollectionName collectionName, ScopeName... scopeNames) {
		deleteById(id, null, collectionName, scopeNames);
	}

	// delete(T entity)

	@Override
	public void delete(T entity) {
		delete(entity, null, null, null);
	}

	@Override
	public void delete(T entity, RemoveOptions... options) {
		delete(entity, null, null, null);
	}

	@Override
	public void delete(T entity, RemoveOptions options, CollectionName collectionName, ScopeName... scopeNames) {
		Assert.notNull(entity, "Entity must not be null!");
		String collName = collectionName == null ? null : collectionName.name();
		couchbaseOperations.removeById().inCollection(collName).one(entityInformation.getId(entity));
	}

	@Override
	public void delete(T entity, CollectionName collectionName, ScopeName... scopeNames) {
		Assert.notNull(entity, "Entity must not be null!");
		String collName = collectionName == null ? null : collectionName.name();
		couchbaseOperations.removeById().inCollection(collName).one(entityInformation.getId(entity));
	}

	@Override
	public void deleteAllById(Iterable<? extends ID> ids) {
		deleteAllById(ids, null, null, null);
	}

	@Override
	public void deleteAllById(Iterable<? extends ID> ids, RemoveOptions... options) {
		deleteAllById(ids, get(options), null, null);
	}

	@Override
	public void deleteAllById(Iterable<? extends ID> ids, RemoveOptions options, CollectionName collectionName,
			ScopeName... scopeNames) {
		Assert.notNull(ids, "The given Iterable of ids must not be null!");
		couchbaseOperations.removeById().inCollection(get(collectionName))
				.all(Streamable.of(ids).map(Objects::toString).toList());
	}

	@Override
	public void deleteAllById(Iterable<? extends ID> ids, CollectionName collectionName, ScopeName... scopeNames) {
		deleteAllById(ids, null, collectionName, scopeNames);
	}

	@Override
	public void deleteAll(Iterable<? extends T> entities) {
		deleteAll(entities, null, null, null);
	}

	@Override
	public void deleteAll(Iterable<? extends T> entities, RemoveOptions... removeOptions) {
		deleteAll(entities, null, null, null);
	}

	@Override
	public void deleteAll(Iterable<? extends T> entities, RemoveOptions options, CollectionName collectionName,
			ScopeName... scopeNames) {
		Assert.notNull(entities, "The given Iterable of entities must not be null!");
		couchbaseOperations.removeById().inCollection(get(collectionName))
				.all(Streamable.of(entities).map(entityInformation::getId).toList());
	}

	@Override
	public void deleteAll(Iterable<? extends T> entities, CollectionName collectionName, ScopeName... scopeNames) {
		deleteAll(entities, null, null, null);
	}

	// public void deleteAll()

	@Override
	public void deleteAll() {
		deleteAll((RemoveOptions) null, null, null);
	}

	@Override
	public void deleteAll(RemoveOptions... options) {
		deleteAll(get(options), null, null);
	}

	@Override
	public void deleteAll(RemoveOptions options, CollectionName collectionName, ScopeName... scopeNames) {
		couchbaseOperations.removeByQuery(entityInformation.getJavaType()).withConsistency(buildQueryScanConsistency())
				.inCollection(get(collectionName)).all();
	}

	@Override
	public void deleteAll(CollectionName collectionName, ScopeName... scopeNames) {
		deleteAll((RemoveOptions) null, null, null);
	}

	/**
	 * @param options -
	 * @param repository - need to pass the repository object, as springframework unwraps the "proxy" from of 'this'
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <T1 extends CouchbaseRepository> T1 withOptions(CommonOptions<?> options, T1 repository,
			Class<?> repositoryClass) {
		T1 proxyInstance = (T1) Proxy.newProxyInstance(this.getClass().getClassLoader(),
				repository.getClass().getInterfaces(),
				new DynamicInvocationHandler(repository, repositoryClass, options, null, (String) null));
		return proxyInstance;
	}

	/**
	 * @param scope - the collection to use
	 * @param repository - need to pass the repository object, as springframework unwraps the "proxy" from of 'this'
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <T1 extends CouchbaseRepository> T1 withScope(String scope, T1 repository, Class<?> repositoryClass) {
		T1 proxyInstance = (T1) Proxy.newProxyInstance(this.getClass().getClassLoader(),
				repository.getClass().getInterfaces(),
				new DynamicInvocationHandler<>(repository, repository.getClass() /*repositoryClass */, null, null, scope));
		return proxyInstance;
	}

	/**
	 * Use collection in template.getClientFactory().getScope()
	 *
	 * @param collection - the collection to use
	 * @param repository - need to pass the repository object, as springframework unwraps the "proxy" from of 'this'
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <T1 extends CouchbaseRepository> T1 withCollection(String collection, T1 repository,
			Class<?> repositoryClass) {
		T1 proxyInstance = (T1) Proxy.newProxyInstance(this.getClass().getClassLoader(),
				repository.getClass().getInterfaces(),
				new DynamicInvocationHandler<>(repository, repositoryClass, null, collection, null));
		return proxyInstance;
	}

	/**
	 * Returns the information for the underlying template.
	 *
	 * @return the underlying entity information.
	 */
	public CouchbaseEntityInformation<T, String> getEntityInformation() {
		return entityInformation;
	}

	private QueryScanConsistency buildQueryScanConsistency() {
		QueryScanConsistency scanConsistency = QueryScanConsistency.NOT_BOUNDED;
		if (crudMethodMetadata.getScanConsistency() != null) {
			scanConsistency = crudMethodMetadata.getScanConsistency().query();
		}
		return scanConsistency;
	}

	/**
	 * Setter for the repository metadata, contains annotations on the overidden methods.
	 *
	 * @param crudMethodMetadata the injected repository metadata.
	 */
	void setRepositoryMethodMetadata(CrudMethodMetadata crudMethodMetadata) {
		this.crudMethodMetadata = crudMethodMetadata;
	}

	<O> O get(O[] args) {
		return args == null || args.length == 0 ? null : args[0];
	}

	String get(CollectionName cName) {
		return cName == null ? null : cName.name();
	}

	String get(ScopeName sName) {
		return sName == null ? null : sName.name();
	}

}
