/*
 * Copyright 2017-2019 the original author or authors.
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

import static java.util.Arrays.*;
import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import com.couchbase.client.core.error.AmbiguousTimeoutException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.IndexFailureException;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.MutationState;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.manager.collection.ScopeSpec;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.RemoveResult;
import org.springframework.data.couchbase.core.support.CollectionName;
import org.springframework.data.couchbase.core.support.ScopeName;
import org.springframework.data.couchbase.domain.Address;
import org.springframework.data.couchbase.domain.Airport;
import org.springframework.data.couchbase.domain.AirportEuropean;
import org.springframework.data.couchbase.domain.AirportRepository;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.domain.PersonRepository;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.couchbase.domain.UserRepository;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterAwareIntegrationTests;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.util.StreamUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.util.Version;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.couchbase.client.core.error.IndexExistsException;
import reactor.core.publisher.Flux;

/**
 * Repository tests
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 * @author Jens Schauder
 */
@SpringJUnitConfig(CouchbaseRepositoryQueryIntegrationTests.Config.class)
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
public class CouchbaseRepositoryQueryIntegrationTests extends ClusterAwareIntegrationTests {

	@Autowired CouchbaseClientFactory clientFactory;

	@Autowired AirportRepository airportRepository;

	@Autowired UserRepository userRepository;

	@Autowired CouchbaseTemplate couchbaseTemplate;

	String collectionName = "_default";
	String scopeName = "_default";

	@BeforeEach
	public void beforeEach() {
		try {
			clientFactory.getCluster().queryIndexes().createPrimaryIndex(bucketName());
		} catch (IndexExistsException ex) {
			// ignore, all good.
		}
	}

	@Test
	public void myTest() {

		JsonObject dave = JsonObject.create().put("id",4).put("iata", "dave_iata").put("icao","dave_icao").put("_class","not Airport");
		couchbaseTemplate.getCouchbaseClientFactory().getBucket().defaultCollection().insert("7",dave);
		Airport air = new Airport("airports::air",				"air",				"loxx");
		AirportEuropean vie = new AirportEuropean("airports::vie", 				"vie",				"loww");
		try {
			Airport airSaved = airportRepository.save(air);
			Airport vieSaved = airportRepository.save(vie);

			Airport vieFetched = airportRepository.findById(vie.getId()).get();
			System.out.println("class:"+vieFetched.getClass()+" "+vieFetched);

			AirportEuropean airport3  = couchbaseTemplate.findById(AirportEuropean.class).one(vie.getId());
			System.out.println("class:"+airport3.getClass()+" "+airport3);

			//  {
			//  "iata": "dave_iata",
			//  "icao": "dave_icao",
			//  "id": 7,
			//  "name": "dave"
			//}
			AirportEuropean airport4  = couchbaseTemplate.findById(AirportEuropean.class).one("7");
			System.out.println("class:"+airport4.getClass()+" "+airport4);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			couchbaseTemplate.getCouchbaseClientFactory().getBucket().defaultCollection().remove("7");
			airportRepository.delete(vie);
			airportRepository.delete(air);
		}
	}

	@Test
	void shouldSaveAndFindAll() {
		Airport vie = null;
		try {
			vie = new Airport("airports::vie", "vie", "loww");
			airportRepository.save(vie);
			List<Airport> all = new ArrayList<>();
			airportRepository.findAll().forEach(all::add);
			assertFalse(all.isEmpty());
			assertTrue(all.stream().anyMatch(a -> a.getId().equals("airports::vie")));
		} finally {
			airportRepository.delete(vie);
		}
	}

	@Autowired PersonRepository personRepository;

	@Test
	void nestedFind() {
		Person person = null;
		try {
			person = new Person(1, "first", "last");
			Address address = new Address();
			address.setStreet("Maple");
			person.setAddress(address);
			personRepository.save(person);
			List<Person> persons = personRepository.findByAddressStreet("Maple");
			assertEquals(1, persons.size());
		} catch (ClassCastException cce) {
			cce.printStackTrace();
		} finally {
			personRepository.deleteById(person.getId().toString());
		}
	}

	// "1\" or name=name or name=\"1")
	@Test
	void findByInjection() {
		Airport vie = null;
		Airport xxx = null;
		try {
			vie = new Airport("airports::vie", "vie", "loww");
			airportRepository.save(vie);
			xxx = new Airport("airports::xxx", "xxx", "xxxx");
			airportRepository.save(xxx);
			List<Airport> airports;
			airports = airportRepository.findAllByIata("1\" or iata=iata or iata=\"1");
			assertEquals(0, airports.size());
			airports = airportRepository.findAllByIata("vie");
			assertEquals(1, airports.size());
		} finally {
			airportRepository.delete(vie);
			airportRepository.delete(xxx);
		}

	}

	@Test
	void findBySimpleProperty() {
		Airport vie = null;
		try {
			vie = new Airport("airports::vie", "vie", "loww");
			vie = airportRepository.save(vie);
			List<Airport> airports = airportRepository.findAllByIata("vie");
			assertEquals(1, airports.size());
			Airport airport1 = airportRepository.findById(airports.get(0).getId()).get();
			assertEquals(airport1.getIata(), vie.getIata());
		} finally {
			airportRepository.delete(vie);
		}
	}

	/**
	 *
	 * 	can test against _default._default without setting up additional scope/collection
	 * 	and also test for collections and scopes that do not exist
	 *
	 * 	These same tests should be repeated on non-default scope and collection in a
	 * 	test that supports collections
	 *
	 */
	@Test
	@IgnoreWhen(missesCapabilities = { Capabilities.QUERY, Capabilities.COLLECTIONS }, clusterTypes = ClusterType.MOCKED)
	void findBySimplePropertyWithCollection() {

		Airport vie = new Airport("airports::vie", "vie", "loww");
		try {
			Airport saved = airportRepository.save(vie);
			// given CollectionSpec
			Airport airport2 = airportRepository.iata(vie.getIata(),
					QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS),
					new CollectionName(collectionName));
			assertEquals(saved, airport2);

			// given ScopeSpec
			Set<CollectionSpec> collectionsSpecs = new HashSet<>();
			collectionsSpecs.add(CollectionSpec.create(collectionName,scopeName));
			Airport airport3 = airportRepository.iata(vie.getIata(),
					QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS),
					new CollectionName(collectionName),
					new ScopeName(scopeName));
			assertEquals(saved, airport3);

			// given ScopeSpec with no collections
			/* this is ok if scopeName == "_default"
			assertThrows( IllegalArgumentException.class, () -> airportRepository.iata(vie.getIata(),
					QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS),
					new ScopeName(scopeName)));
			 */
			// given bad collectionName in CollectionSpec
			assertThrows( IndexFailureException.class, () ->airportRepository.iata(vie.getIata(),
					QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS),
					new CollectionName("bogusCollection")));

			// given bad scopeName in ScopeSpec
			Set<CollectionSpec> collectionsSpecs1 = new HashSet<>();
			collectionsSpecs1.add(CollectionSpec.create(collectionName,scopeName));
			assertThrows( IndexFailureException.class, ()-> airportRepository.iata(vie.getIata(),
					QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS),
					new CollectionName(collectionName),
					new ScopeName("bogusScope")));

			} finally {
			airportRepository.delete(vie);
		}
	}

	@Test
	@IgnoreWhen(hasCapabilities = { Capabilities.COLLECTIONS } , clusterTypes = ClusterType.MOCKED)
	void findBySimplePropertyWithCollectionFail() {
		// can test against _default._default without setting up additional scope/collection
		// the server will throw an exception if it doesn't support COLLECTIONS
		Airport vie = new Airport("airports::vie", "vie", "loww");
		try {

			Airport saved = airportRepository.save(vie);

			Set<CollectionSpec> collectionsSpecs = new HashSet<>();
			collectionsSpecs.add(CollectionSpec.create(collectionName,scopeName));
			assertThrows( CouchbaseException.class, () ->  airportRepository.iata(vie.getIata(),
					QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS),
					new CollectionName(collectionName),
					new ScopeName("non_default_scope_name")));

		} finally {
			airportRepository.delete(vie);
		}
	}

	@Test
	void findBySimplePropertyWithOptions() {

		Airport vie = new Airport("airports::vie", "vie", "loww");
		JsonArray positionalParams = JsonArray.create().add(vie.getIata());
		//JsonObject namedParams = JsonObject.create().put("$1", vie.getIata());
		try {
			Airport saved = airportRepository.save(vie);
			// Duration of 1 nano-second will cause timeout
			assertThrows(AmbiguousTimeoutException.class,
					() -> airportRepository.iata(vie.getIata(), QueryOptions.queryOptions().timeout(Duration.ofNanos(1))));

			Airport airport3 = airportRepository.iata("this parameter will be overridden",
					QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS).parameters(positionalParams));
			assertEquals(saved,airport3);

		} finally {
			airportRepository.delete(vie);
		}

		// save() followed by query with NOT_BOUNDED will result in not finding the document
		Airport airport2 = null;
		for (int i = 1; i <= 10; i++) {
			Airport saved = airportRepository.save(vie);
			try {
				airport2 = airportRepository.iata(saved.getIata(),
						QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.NOT_BOUNDED).parameters(positionalParams));
				if (airport2 == null) {
					break;
				}
			} finally {
				// airportRepository.delete(vie);
				// instead of delete, use removeResult to test QueryOptions.consistentWith()
				RemoveResult removeResult = couchbaseTemplate.removeById().one(vie.getId());
				assertEquals(vie.getId(), removeResult.getId());
				assertTrue(removeResult.getCas() != 0);
				assertTrue(removeResult.getMutationToken().isPresent());
				Airport airport3 = airportRepository.iata(vie.getIata(),
						QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS)
								.consistentWith(MutationState.from(removeResult.getMutationToken().get())));
				assertNull(airport3, "should have been removed");
			}
		}
		assertNull(airport2, "airport2 should have likely been null at least once");

	}

	@Test
	public void testCas() {
		User user = new User("1", "Dave", "Wilson");
		userRepository.save(user);
		user.setVersion(user.getVersion() - 1);
		assertThrows(DataIntegrityViolationException.class, () -> userRepository.save(user));
		user.setVersion(0);
		userRepository.save(user);
		userRepository.delete(user);
	}

	@Test
	void count() {
		String[] iatas = { "JFK", "IAD", "SFO", "SJC", "SEA", "LAX", "PHX" };

		try {

			airportRepository.saveAll(
					Arrays.stream(iatas).map((iata) -> new Airport("airports::" + iata, iata, iata.toLowerCase(Locale.ROOT)))
							.collect(Collectors.toSet()));
			Long count = airportRepository.countFancyExpression(asList("JFK"), asList("jfk"), false);
			assertEquals(1, count);

			Pageable pageable = PageRequest.of(0, 2);
			Page<Airport> aPage = airportRepository.findAllByIataNot("JFK", pageable);
			assertEquals(iatas.length - 1, aPage.getTotalElements());
			assertEquals(pageable.getPageSize(), aPage.getContent().size());

			long airportCount = airportRepository.count();
			assertEquals(7, airportCount);

			airportCount = airportRepository.countByIataIn("JFK", "IAD", "SFO");
			assertEquals(3, airportCount);

			airportCount = airportRepository.countByIcaoAndIataIn("jfk", "JFK", "IAD", "SFO", "XXX");
			assertEquals(1, airportCount);

			airportCount = airportRepository.countByIcaoOrIataIn("jfk", "LAX", "IAD", "SFO");
			assertEquals(4, airportCount);

			airportCount = airportRepository.countByIataIn("XXX");
			assertEquals(0, airportCount);

		} finally {
			airportRepository
					.deleteAllById(Arrays.stream(iatas).map((iata) -> "airports::" + iata).collect(Collectors.toSet()));
		}
	}

	@Test
	void threadSafeParametersTest() throws Exception {
		String[] iatas = { "JFK", "IAD", "SFO", "SJC", "SEA", "LAX", "PHX" };
		Future[] future = new Future[iatas.length];
		ExecutorService executorService = Executors.newFixedThreadPool(iatas.length);

		try {
			for (int i = 0; i < iatas.length; i++) {
				Airport airport = new Airport("airports::" + iatas[i], iatas[i] /*iata*/,
						iatas[i].toLowerCase(Locale.ROOT) /* lcao */);
				airportRepository.save(airport);
			}

			for (int k = 0; k < 50; k++) {
				Callable<Boolean>[] suppliers = new Callable[iatas.length];
				for (int i = 0; i < iatas.length; i++) {
					final int idx = i;
					suppliers[i] = () -> {
						sleep(iatas.length - idx); // so they are executed out-of-order
						List<Airport> airports = airportRepository.findAllByIata(iatas[idx]);
						String foundName = airportRepository.findAllByIata(iatas[idx]).get(0).getIata();
						assertEquals(iatas[idx], foundName);
						return iatas[idx].equals(foundName);
					};
				}
				for (int i = 0; i < iatas.length; i++) {
					future[i] = executorService.submit(suppliers[i]);
				}
				for (int i = 0; i < iatas.length; i++) {
					future[i].get(); // check is done in Callable
				}
			}

		} finally {
			executorService.shutdown();
			for (int i = 0; i < iatas.length; i++) {
				Airport airport = new Airport("airports::" + iatas[i], iatas[i] /*iata*/, iatas[i] /* lcao */);
				airportRepository.delete(airport);
			}
		}
	}

	@Test
	void threadSafeStringParametersTest() throws Exception {
		String[] iatas = { "JFK", "IAD", "SFO", "SJC", "SEA", "LAX", "PHX" };
		Future[] future = new Future[iatas.length];
		ExecutorService executorService = Executors.newFixedThreadPool(iatas.length);

		try {
			for (int i = 0; i < iatas.length; i++) {
				Airport airport = new Airport("airports::" + iatas[i], iatas[i] /*iata*/, iatas[i].toLowerCase() /* lcao */);
				airportRepository.save(airport);
			}

			for (int k = 0; k < 100; k++) {
				Callable<Boolean>[] suppliers = new Callable[iatas.length];
				for (int i = 0; i < iatas.length; i++) {
					final int idx = i;
					suppliers[i] = () -> {
						sleep(iatas.length - idx); // so they are executed out-of-order
						String foundName = airportRepository.getAllByIata(iatas[idx]).get(0).getIata();
						assertEquals(iatas[idx], foundName);
						return iatas[idx].equals(foundName);
					};
				}
				for (int i = 0; i < iatas.length; i++) {
					future[i] = executorService.submit(suppliers[i]);
				}
				for (int i = 0; i < iatas.length; i++) {
					future[i].get(); // check is done in Callable
				}
			}
		} finally {
			executorService.shutdown();
			for (int i = 0; i < iatas.length; i++) {
				Airport airport = new Airport("airports::" + iatas[i], iatas[i] /*iata*/, iatas[i] /* lcao */);
				airportRepository.delete(airport);
			}
		}
	}

	@Test // DATACOUCH-650
	void deleteAllById() {

		Airport vienna = new Airport("airports::vie", "vie", "LOWW");
		Airport frankfurt = new Airport("airports::fra", "fra", "EDDF");
		Airport losAngeles = new Airport("airports::lax", "lax", "KLAX");

		try {
			airportRepository.saveAll(asList(vienna, frankfurt, losAngeles));

			airportRepository.deleteAllById(asList(vienna.getId(), losAngeles.getId()));

			assertThat(airportRepository.findAll()).containsExactly(frankfurt);
		} finally {
			airportRepository.deleteAll();
		}
	}

	private void sleep(int millis) {
		try {
			Thread.sleep(millis); // so they are executed out-of-order
		} catch (InterruptedException ie) {
			;
		}
	}

	@Configuration
	@EnableCouchbaseRepositories("org.springframework.data.couchbase")
	static class Config extends AbstractCouchbaseConfiguration {

		@Override
		public String getConnectionString() {
			return connectionString();
		}

		@Override
		public String getUserName() {
			return config().adminUsername();
		}

		@Override
		public String getPassword() {
			return config().adminPassword();
		}

		@Override
		public String getBucketName() {
			return bucketName();
		}

		@Override
		public void configureEnvironment(final ClusterEnvironment.Builder builder) {
			builder.ioConfig().maxHttpConnections(11).idleHttpConnectionTimeout(Duration.ofSeconds(2));
			return;
		}

	}

}
