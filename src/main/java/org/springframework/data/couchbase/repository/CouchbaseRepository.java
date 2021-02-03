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

package org.springframework.data.couchbase.repository;

import java.util.List;
import java.util.Optional;

import com.couchbase.client.java.Collection;
import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.ReplaceOptions;
import com.couchbase.client.java.kv.UpsertOptions;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.manager.collection.ScopeSpec;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;
import org.springframework.data.couchbase.core.support.CollectionName;
import org.springframework.data.couchbase.core.support.ScopeName;
import org.springframework.data.couchbase.repository.query.CouchbaseEntityInformation;
import org.springframework.data.couchbase.repository.support.SimpleCouchbaseRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.NoRepositoryBean;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.Repository;

/**
 * Couchbase specific {@link Repository} interface.
 *
 * @author Michael Nitschinger
 */
@NoRepositoryBean
public interface CouchbaseRepository<T, ID> extends PagingAndSortingRepository<T, ID> {

	// all the inherited methods need to be supplemented by ones that take Options, CollectionName and ScopeName args

	@Override
	Optional<T> findById(ID id);
	Optional<T> findById(ID id, QueryOptions... options);
	Optional<T> findById(ID id, QueryOptions options, CollectionName collectionName, ScopeName... scopeNames);
	Optional<T> findById(ID id, CollectionName collectionName, ScopeName... scopeNames);

	@Override
	List<T> findAll();
	List<T> findAll(QueryOptions... options);
	List<T> findAll(QueryOptions options, CollectionName collectionName, ScopeName... scopeNames);
	List<T> findAll(CollectionName collectionName, ScopeName... scopeNames);


	List<T> findAll(org.springframework.data.couchbase.core.query.Query query);
	List<T> findAll(org.springframework.data.couchbase.core.query.Query query, QueryOptions... options);
	List<T> findAll(org.springframework.data.couchbase.core.query.Query query, QueryOptions options, CollectionName collectionName, ScopeName... scopeNames);
	List<T> findAll(org.springframework.data.couchbase.core.query.Query query, CollectionName collectionName, ScopeName... scopeNames);

	@Override
	List<T> findAll(Sort sort);
	List<T> findAll(Sort sort, QueryOptions... options);
	List<T> findAll(Sort sort, QueryOptions options, CollectionName collectionName, ScopeName... scopeNames);
	List<T> findAll(Sort sort, CollectionName collectionName, ScopeName... scopeNames);

	@Override
	Page<T> findAll(Pageable pageable);
	Page<T> findAll(Pageable pageable, QueryOptions... options);
	Page<T> findAll(Pageable pageable, QueryOptions options, CollectionName collectionName, ScopeName... scopeNames);
	Page<T> findAll(Pageable pageable, CollectionName collectionName, ScopeName... scopeNames);

	List<T> findAll(QueryScanConsistency queryScanConsistency);
	List<T> findAll(QueryScanConsistency queryScanConsistency, QueryOptions... options);
	List<T> findAll(QueryScanConsistency queryScanConsistency, QueryOptions options, CollectionName collectionName, ScopeName... scopeNames);
	List<T> findAll(QueryScanConsistency queryScanConsistency, CollectionName collectionName, ScopeName... scopeNames);

	@Override
	List<T> findAllById(Iterable<ID> iterable);
	List<T> findAllById(Iterable<ID> iterable, QueryOptions... options);
	List<T> findAllById(Iterable<ID> iterable, QueryOptions options, CollectionName collectionName, ScopeName... scopeNames);
	List<T> findAllById(Iterable<ID> iterable, CollectionName collectionName, ScopeName... scopeNames);

	@Override
	boolean existsById(ID id);
	boolean existsById(ID id, QueryOptions... options);
	boolean existsById(ID id, QueryOptions options, CollectionName collectionName, ScopeName... scopeNames);
	boolean existsById(ID id, CollectionName collectionName, ScopeName... scopeNames);

	@Override
	long count();
	long count(QueryOptions... options);
	long count(QueryOptions options, CollectionName collectionName, ScopeName... scopeName);
	long count(CollectionName collectionName, ScopeName... scopeName);

	@Override
	<S extends T> S save(S entity);
	<S extends T> S save(S entity, UpsertOptions... options);
	<S extends T> S save(S entity, ReplaceOptions... options);
	<S extends T> S save(S entity, UpsertOptions options, CollectionName collectionName, ScopeName... scopeNames);
	<S extends T> S save(S entity, ReplaceOptions options, CollectionName collectionName, ScopeName... scopeNames);
	<S extends T> S save(S entity, CollectionName collectionName, ScopeName... scopeNames);

	@Override
	<S extends T> Iterable<S> saveAll(Iterable<S> entities);
	<S extends T> Iterable<S> saveAll(Iterable<S> entities, UpsertOptions... options);
	<S extends T> Iterable<S> saveAll(Iterable<S> entities, ReplaceOptions... options);
	<S extends T> Iterable<S> saveAll(Iterable<S> entities, UpsertOptions options, CollectionName collectionName,
																		ScopeName... scopeNames);
	<S extends T> Iterable<S> saveAll(Iterable<S> entities, ReplaceOptions options, CollectionName collectionName,
			ScopeName... scopeNames);
	<S extends T> Iterable<S> saveAll(Iterable<S> entities, CollectionName collectionName, ScopeName... scopeName);

	@Override
	void deleteById(ID id);
	void deleteById(ID id, RemoveOptions... options);
	void deleteById(ID id, RemoveOptions options, CollectionName collectionName, ScopeName... scopeNames);
	void deleteById(ID id,  CollectionName collectionName, ScopeName... scopeNames);

	@Override
	void delete(T entity);
	void delete(T entity, RemoveOptions... options);
	void delete(T entity, RemoveOptions options, CollectionName collectionName, ScopeName... scopeNames);
	void delete(T entity, CollectionName collectionName, ScopeName... scopeNames);

	@Override
	void deleteAllById(Iterable<? extends ID> ids);
	void deleteAllById(Iterable<? extends ID> ids, RemoveOptions... options);
	void deleteAllById(Iterable<? extends ID> ids, RemoveOptions options, CollectionName collectionName, ScopeName... scopeNames);
	void deleteAllById(Iterable<? extends ID> ids, CollectionName collectionName, ScopeName... scopeNames);

	@Override
	void deleteAll(Iterable<? extends T> entities);
	void deleteAll(Iterable<? extends T> entities, RemoveOptions... removeOptions);
	void deleteAll(Iterable<? extends T> entities, RemoveOptions removeOptions, CollectionName collectionName,
								 ScopeName... scopeNames);
	void deleteAll(Iterable<? extends T> entities, CollectionName collectionName,
								 ScopeName... scopeNames);

	@Override
	void deleteAll();
	void deleteAll(RemoveOptions... removeOptions);
	void deleteAll(RemoveOptions removeOptions, CollectionName collectionName, ScopeName... scopeNames);
	void deleteAll(CollectionName collectionName, ScopeName... scopeNames);

	/**
	 * implemented in SimpleCouchbaseRepository They need to be called from the Actual repository Interface with the
	 * interface object passed as theRepository
	 */
	<T1 extends CouchbaseRepository> T1 withOptions(CommonOptions<?> options, T1 theRepository, Class<?> repositoryClass);

	<T1 extends CouchbaseRepository> T1 withCollection(String collection, T1 theRepository, Class<?> repositoryClass);

	<T1 extends CouchbaseRepository> T1 withScope(String scope, T1 theRepository, Class<?> repositoryClass);

	// copy these three methods into your Repository inteface class, replacing "CouchbaseRepository.class" with that
	// of your repository.
	default CouchbaseRepository<T, ID> withOptions(QueryOptions options) {
		return withOptions(options, this, CouchbaseRepository.class);
	}

	default CouchbaseRepository<T, ID> withCollection(String collection) {
		return withCollection(collection, this, CouchbaseRepository.class);
	}

	default CouchbaseRepository<T, ID> withScope(String scope) {
		return withScope(scope, this, CouchbaseRepository.class);
	}

	 CouchbaseEntityInformation<T, String> getEntityInformation();

}
