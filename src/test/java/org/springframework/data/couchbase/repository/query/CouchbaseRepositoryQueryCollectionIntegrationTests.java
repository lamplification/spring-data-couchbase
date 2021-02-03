package org.springframework.data.couchbase.repository.query;

import com.couchbase.client.core.error.IndexFailureException;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.core.query.QueryCriteria;
import org.springframework.data.couchbase.core.support.CollectionName;
import org.springframework.data.couchbase.core.support.ScopeName;
import org.springframework.data.couchbase.domain.Airport;
import org.springframework.data.couchbase.domain.AirportRepository;
import org.springframework.data.couchbase.domain.Config;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.couchbase.domain.UserRepository;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.CollectionAwareIntegrationTests;
import org.springframework.data.couchbase.util.IgnoreWhen;

import java.math.BigInteger;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.data.couchbase.core.query.N1QLExpression.meta;
import static org.springframework.data.couchbase.core.query.N1QLExpression.path;
import static org.springframework.data.couchbase.repository.query.support.N1qlUtils.escapedBucket;

@IgnoreWhen(missesCapabilities = { Capabilities.QUERY, Capabilities.COLLECTIONS }, clusterTypes = ClusterType.MOCKED)
public class CouchbaseRepositoryQueryCollectionIntegrationTests extends CollectionAwareIntegrationTests {

	@Autowired CouchbaseClientFactory clientFactory;

	@Autowired AirportRepository airportRepository;

	@Autowired UserRepository userRepository;

	@BeforeAll
	public static void beforeAll() {
		// first call the super method
		callSuperBeforeAll(new Object() {});
		// then do processing for this class
	}

	@AfterAll
	public static void afterAll() {
		// first do the processing for this class
		// no-op
		// then call the super method
		callSuperAfterAll(new Object() {});
	}

	@BeforeEach
	@Override
	public void beforeEach() {
		// first call the super method
		super.beforeEach();
		// then do processing for this class
		couchbaseTemplate.removeByQuery(User.class).inCollection(collectionName).all();
		ApplicationContext ac = new AnnotationConfigApplicationContext(Config.class);
		// seems that @Autowired is not adequate, so ...
		airportRepository = (AirportRepository) ac.getBean("airportRepository");
	}

	@AfterEach
	@Override
	public void afterEach() {
		// first do processing for this class
		// no-op
		// then call the super method
		super.afterEach();
	}

	@Test
	public void myTest(){

		Airport vie = new Airport("airports::vie", "vie", "loww");
		try {
			// repository.withCollection()
			airportRepository = airportRepository.withCollection(collectionName);
			Airport saved = airportRepository.save(vie);
			// given CollectionSpec
			// repository.someMethod(ar1, ar2, QueryOptions CollectionSpec)
			Airport airport2 = airportRepository.save(saved);
			System.out.println(airport2);
			//String result = couchbaseTemplate.replaceById(Airport.class).one(saved);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			airportRepository.delete(vie);
		}
		/*
		UPDATE `YS_Apps`
		USE KEYS "TEST:ALPHA:001d3b5f-31f5-4d5b-a580-5e2283a2510b"
		SET `deleted` = TRUE
		WHERE (`deleted` = false and (META(`YS_Apps`).cas = 1611361192568946688)) AND `_class` = "com.yapstone.rest.framework.couchbase.repository.AlphaTestEntity"
		RETURNING `YS_Apps`.*, META(`YS_Apps`).id AS __id, META(`YS_Apps`).cas AS __cas
				*/
		/*

		BigInteger version =  new BigInteger("1612203204822368300");
		String versionString=String.valueOf(version);
		QueryCriteria criteria0 = QueryCriteria.where(path(meta(escapedBucket("my_bucket")), "cas").toString()).eq(version);
		System.out.println(criteria0.export());

		QueryCriteria criteria1 = QueryCriteria.where(versionString).eq(path(meta(escapedBucket("my_bucket")), "cas"));
		System.out.println(criteria1.export());
		 */
	}
	/**
	 * can test against _default._default without setting up additional scope/collection and also test for collections and
	 * scopes that do not exist These same tests should be repeated on non-default scope and collection in a test that
	 * supports collections
	 */
	@Test
	@IgnoreWhen(missesCapabilities = { Capabilities.QUERY, Capabilities.COLLECTIONS }, clusterTypes = ClusterType.MOCKED)
	void findBySimplePropertyWithCollection() {

		Airport vie = new Airport("airports::vie", "vie", "loww");
		try {
			// repository.withCollection()
			Airport saved = airportRepository.withScope(scopeName).withCollection(collectionName).save(vie);
			// given CollectionSpec
			// repository.someMethod(ar1, ar2, QueryOptions CollectionSpec)
			Airport airport2 = airportRepository.withScope(scopeName).withCollection(collectionName).iata(vie.getIata(),
					/*QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS),*/ null,
					new CollectionName(collectionName));
			assertEquals(saved, airport2);

			// given ScopeSpec with no collections
			// the scope will add query_context , but without a collection, it will use the bucketname in the query
			assertThrows(IndexFailureException.class,
					() -> airportRepository.iata(vie.getIata(),
							QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS),
							new CollectionName("bogusCollection"), new ScopeName(scopeName)));

			// given bad collectionName in CollectionSpec
			assertThrows(IndexFailureException.class,
					() -> airportRepository.iata(vie.getIata(),
							QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS),
							new CollectionName("bogusCollection")));

			Airport airport6 = airportRepository.iata(vie.getIata(),
					QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS),
					new CollectionName(collectionName)/*, new ScopeName(scopeName) what goes on here? */);
			assertEquals(saved, airport6);

			// given bad scopeName in ScopeSpec
			Set<CollectionSpec> collectionsSpecs1 = new HashSet<>();
			collectionsSpecs1.add(CollectionSpec.create(collectionName, scopeName));
			assertThrows(IndexFailureException.class,
					() -> airportRepository.iata(vie.getIata(),
							QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS),
							new CollectionName(collectionName), new ScopeName("bogusScopeName")));

		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			List<Airport> list = new LinkedList();
			list.add(vie);
			airportRepository.withScope(scopeName).withCollection(collectionName).deleteAll(list);
			airportRepository.withCollection(collectionName).deleteAll();
			airportRepository.withCollection(collectionName).deleteAll(RemoveOptions.removeOptions());
			airportRepository.withCollection(collectionName).deleteAll(RemoveOptions.removeOptions(), new CollectionName(collectionName));
			airportRepository.withCollection(collectionName).deleteAll(RemoveOptions.removeOptions(), new CollectionName(collectionName), new ScopeName(scopeName));
			//airportRepository.withScope(scopeName).withCollection(collectionName).delete(vie);
		}
	}

	@Test
	void findBySimplePropertyWithOptions() {

		Airport vie = new Airport("airports::vie", "vie", "loww");
		JsonArray positionalParams = JsonArray.create().add(vie.getIata());
		try {
			// Airport saved2 = airportRepository.withScope(scopeName).save(vie);
			Airport saved = airportRepository.withCollection(collectionName).save(vie);

			Airport airport3 = airportRepository.withCollection(collectionName).iata("this parameter will be overridden",
					QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS).parameters(positionalParams));
			assertEquals(saved.getIata(), airport3.getIata());

		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			airportRepository.withCollection(collectionName).delete(vie);
		}
	}
}
