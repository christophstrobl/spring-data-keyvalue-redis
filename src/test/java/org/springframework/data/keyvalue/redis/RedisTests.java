/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.keyvalue.redis;

import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsCollectionContaining.*;
import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.hamcrest.core.IsNull;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Reference;
import org.springframework.data.keyvalue.annotation.KeySpace;
import org.springframework.data.keyvalue.core.KeyValueTemplate;
import org.springframework.data.keyvalue.redis.core.index.IndexConfiguration;
import org.springframework.data.keyvalue.redis.core.index.Indexed;
import org.springframework.data.keyvalue.redis.core.index.RedisIndexDefinition;
import org.springframework.data.keyvalue.redis.repository.config.EnableRedisRepositories;
import org.springframework.data.repository.CrudRepository;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Christoph Strobl
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class RedisTests {

	@Configuration
	@EnableRedisRepositories(considerNestedRepositories = true, indexConfiguration = MyIndexConfiguration.class)
	static class Config {

	}

	@Autowired PersonRepository repo;
	@Autowired KeyValueTemplate kvTemplate;

	@Before
	public void setUp() {
		repo.deleteAll();
	}

	@Test
	public void findByNameStartingWith() {

		Person rand = new Person();
		rand.firstname = "rand";
		rand.lastname = "al'thor";

		Person egwene = new Person();
		egwene.firstname = "egwene";

		repo.save(Arrays.asList(rand, egwene));

		assertThat(repo.count(), is(2L));

		assertThat(repo.findOne(rand.id), is(rand));
		assertThat(repo.findOne(egwene.id), is(egwene));

		assertThat(repo.findByFirstname("rand").size(), is(1));
		assertThat(repo.findByFirstname("rand"), hasItem(rand));

		assertThat(repo.findByLastname("al'thor"), hasItem(rand));

	}

	@Test
	public void findReturnsReferenceDataCorrectly() {

		// flush keyspaces
		kvTemplate.delete(Person.class);
		kvTemplate.delete(Location.class);

		// Prepare referenced data entry
		Location tarValon = new Location();
		tarValon.id = "1";
		tarValon.name = "tar valon";

		kvTemplate.insert(tarValon);

		// Prepare domain entity
		Person moiraine = new Person();
		moiraine.firstname = "moiraine";
		moiraine.currentLocation = tarValon; // reference data

		// save domain entity
		repo.save(moiraine);

		// find and assert current location set correctly
		Person loaded = repo.findOne(moiraine.getId());
		assertThat(loaded.currentLocation, is(tarValon));

		// remove reference location data
		kvTemplate.delete("1", Location.class);

		// find and assert the location is gone
		Person reLoaded = repo.findOne(moiraine.getId());
		assertThat(reLoaded.currentLocation, IsNull.nullValue());
	}

	public static interface PersonRepository extends CrudRepository<Person, String> {

		List<Person> findByLastname(String lastname);

		List<Person> findByFirstname(String firstname);
	}

	static class MyIndexConfiguration extends IndexConfiguration {

		@Override
		protected Iterable<RedisIndexDefinition> initialConfiguration() {
			return Collections.singleton(new RedisIndexDefinition("persons", "lastname"));
		}
	}

	@KeySpace("persons")
	@SuppressWarnings("serial")
	public static class Person implements Serializable {

		@Id String id;
		@Indexed String firstname;
		String lastname;
		@Reference Location currentLocation;

		public Location getCurrentLocation() {
			return currentLocation;
		}

		public void setCurrentLocation(Location currentLocation) {
			this.currentLocation = currentLocation;
		}

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getFirstname() {
			return firstname;
		}

		public void setFirstname(String firstname) {
			this.firstname = firstname;
		}

		public void setLastname(String lastname) {
			this.lastname = lastname;
		}

		public String getLastname() {
			return lastname;
		}

		@Override
		public String toString() {
			return "Person [id=" + id + ", firstname=" + firstname + "]";
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((firstname == null) ? 0 : firstname.hashCode());
			result = prime * result + ((id == null) ? 0 : id.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (!(obj instanceof Person)) {
				return false;
			}
			Person other = (Person) obj;
			if (firstname == null) {
				if (other.firstname != null) {
					return false;
				}
			} else if (!firstname.equals(other.firstname)) {
				return false;
			}
			if (id == null) {
				if (other.id != null) {
					return false;
				}
			} else if (!id.equals(other.id)) {
				return false;
			}
			return true;
		}

	}

	@KeySpace("locations")
	public static class Location {
		@Id String id;
		String name;

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((id == null) ? 0 : id.hashCode());
			result = prime * result + ((name == null) ? 0 : name.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (!(obj instanceof Location)) {
				return false;
			}
			Location other = (Location) obj;
			if (id == null) {
				if (other.id != null) {
					return false;
				}
			} else if (!id.equals(other.id)) {
				return false;
			}
			if (name == null) {
				if (other.name != null) {
					return false;
				}
			} else if (!name.equals(other.name)) {
				return false;
			}
			return true;
		}
	}

}
