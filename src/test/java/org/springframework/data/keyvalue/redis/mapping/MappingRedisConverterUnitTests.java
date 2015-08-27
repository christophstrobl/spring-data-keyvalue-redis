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
package org.springframework.data.keyvalue.redis.mapping;

import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsCollectionContaining.*;
import static org.hamcrest.core.IsInstanceOf.*;
import static org.hamcrest.core.IsNull.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.keyvalue.annotation.KeySpace;
import org.springframework.data.keyvalue.redis.convert.MappingRedisConverter;
import org.springframework.data.keyvalue.redis.convert.RedisDataObject;

public class MappingRedisConverterUnitTests {

	MappingRedisConverter converter;
	Person rand;

	@Before
	public void setUp() {

		converter = new MappingRedisConverter();
		rand = new Person();

	}

	@Test
	public void writeAppendsTypeHintForRootCorrectly() {
		assertThat(write(rand).hashAsUtf8String().get("_class"), is(Person.class.getName()));
	}

	@Test
	public void writeAppendsKeyCorrectly() {

		rand.id = "1";

		assertThat(write(rand).keyAsUtf8String(), is("persons:1"));
	}

	@Test
	public void writeDoesNotAppendPropertiesWithNullValues() {

		rand.firstname = "rand";

		assertThat(write(rand).hashAsUtf8String().containsKey("lastname"), is(false));
	}

	@Test
	public void writeDoesNotAppendPropertiesWithEmtpyCollections() {

		rand.firstname = "rand";

		assertThat(write(rand).hashAsUtf8String().containsKey("nicknames"), is(false));
	}

	@Test
	public void writeAppendsSimpleRootPropertyCorrectly() {

		rand.firstname = "nynaeve";

		assertThat(write(rand).hashAsUtf8String().get("firstname"), is("nynaeve"));
	}

	@Test
	public void writeAppendsListOfSimplePropertiesCorrectly() {

		rand.nicknames = Arrays.asList("dragon reborn", "lews therin");

		RedisDataObject target = write(rand);

		assertThat(target.hashAsUtf8String().get("nicknames.[0]"), is("dragon reborn"));
		assertThat(target.hashAsUtf8String().get("nicknames.[1]"), is("lews therin"));
	}

	@Test
	public void writeAppendsComplexObjectCorrectly() {

		Address address = new Address();
		address.city = "two rivers";
		address.country = "andora";
		rand.address = address;

		RedisDataObject target = write(rand);

		assertThat(target.hashAsUtf8String().get("address.city"), is("two rivers"));
		assertThat(target.hashAsUtf8String().get("address.country"), is("andora"));
	}

	@Test
	public void writeAppendsListOfComplexObjectsCorrectly() {

		Person mat = new Person();
		mat.firstname = "mat";
		mat.nicknames = Arrays.asList("prince of the ravens");

		Person perrin = new Person();
		perrin.firstname = "perrin";
		perrin.address = new Address();
		perrin.address.city = "two rivers";

		rand.coworkers = Arrays.asList(mat, perrin);
		rand.id = UUID.randomUUID().toString();
		rand.firstname = "rand";

		RedisDataObject target = write(rand);

		assertThat(target.hashAsUtf8String().get("coworkers.[0].firstname"), is("mat"));
		assertThat(target.hashAsUtf8String().get("coworkers.[0].nicknames.[0]"), is("prince of the ravens"));
		assertThat(target.hashAsUtf8String().get("coworkers.[1].firstname"), is("perrin"));
		assertThat(target.hashAsUtf8String().get("coworkers.[1].address.city"), is("two rivers"));
	}

	@Test
	public void writeDoesNotAddClassTypeInformationCorrectlyForMatchingTypes() {

		Address address = new Address();
		address.city = "two rivers";

		rand.address = address;

		RedisDataObject target = write(rand);

		assertThat(target.hashAsUtf8String().containsKey("address._class"), is(false));
	}

	@Test
	public void writeAddsClassTypeInformationCorrectlyForNonMatchingTypes() {

		AddressWithPostcode address = new AddressWithPostcode();
		address.city = "two rivers";
		address.postcode = "1234";

		rand.address = address;

		RedisDataObject target = write(rand);

		assertThat(target.hashAsUtf8String().get("address._class"), is(AddressWithPostcode.class.getName()));
	}

	@Test
	public void readConsidersClassTypeInformationCorrectlyForNonMatchingTypes() {

		Map<String, String> map = new HashMap<String, String>();
		map.put("address._class", AddressWithPostcode.class.getName());
		map.put("address.postcode", "1234");

		Person target = converter.read(Person.class, RedisDataObject.fromStringMap(map));

		assertThat(target.address, instanceOf(AddressWithPostcode.class));
	}

	@Test
	public void writeAddsClassTypeInformationCorrectlyForNonMatchingTypesInCollections() {

		Person mat = new TaVeren();
		mat.firstname = "mat";

		rand.coworkers = Arrays.asList(mat);

		RedisDataObject target = write(rand);

		assertThat(target.hashAsUtf8String().get("coworkers.[0]._class"), is(TaVeren.class.getName()));
	}

	@Test
	public void readConvertsSimplePropertiesCorrectly() {

		RedisDataObject rdo = RedisDataObject.fromStringMap(Collections.singletonMap("firstname", "rand"));

		assertThat(converter.read(Person.class, rdo).firstname, is("rand"));
	}

	@Test
	public void readConvertsListOfSimplePropertiesCorrectly() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("nicknames.[0]", "dragon reborn");
		map.put("nicknames.[1]", "lews therin");
		RedisDataObject rdo = RedisDataObject.fromStringMap(map);

		assertThat(converter.read(Person.class, rdo).nicknames, hasItems("dragon reborn", "lews therin"));
	}

	@Test
	public void readComplexPropertyCorrectly() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("address.city", "two rivers");
		map.put("address.country", "andor");
		RedisDataObject rdo = RedisDataObject.fromStringMap(map);

		Person target = converter.read(Person.class, rdo);

		assertThat(target.address, notNullValue());
		assertThat(target.address.city, is("two rivers"));
		assertThat(target.address.country, is("andor"));
	}

	@Test
	public void readListComplexPropertyCorrectly() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("coworkers.[0].firstname", "mat");
		map.put("coworkers.[0].nicknames.[0]", "prince of the ravens");
		map.put("coworkers.[1].firstname", "perrin");
		map.put("coworkers.[1].address.city", "two rivers");
		RedisDataObject rdo = RedisDataObject.fromStringMap(map);

		Person target = converter.read(Person.class, rdo);

		assertThat(target.coworkers, notNullValue());
		assertThat(target.coworkers.get(0).firstname, is("mat"));
		assertThat(target.coworkers.get(0).nicknames, notNullValue());
		assertThat(target.coworkers.get(0).nicknames.get(0), is("prince of the ravens"));

		assertThat(target.coworkers.get(1).firstname, is("perrin"));
		assertThat(target.coworkers.get(1).address.city, is("two rivers"));
	}

	@Test
	public void readListComplexPropertyCorrectlyAndConsidersClassTypeInformation() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("coworkers.[0]._class", TaVeren.class.getName());
		map.put("coworkers.[0].firstname", "mat");

		RedisDataObject rdo = RedisDataObject.fromStringMap(map);

		Person target = converter.read(Person.class, rdo);

		assertThat(target.coworkers, notNullValue());
		assertThat(target.coworkers.get(0), instanceOf(TaVeren.class));
		assertThat(target.coworkers.get(0).firstname, is("mat"));
	}

	@Test
	public void writeAppendsMapWithSimpleKeyCorrectly() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("hair-color", "red");
		map.put("eye-color", "grey");

		rand.physicalAttributes = map;

		RedisDataObject target = write(rand);

		assertThat(target.hashAsUtf8String().get("physicalAttributes.[hair-color]"), is("red"));
		assertThat(target.hashAsUtf8String().get("physicalAttributes.[eye-color]"), is("grey"));
	}

	@Test
	public void writeAppendsMapWithSimpleKeyOnNestedObjectCorrectly() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("hair-color", "red");
		map.put("eye-color", "grey");

		rand.coworkers = new ArrayList<Person>();
		rand.coworkers.add(new Person());
		rand.coworkers.get(0).physicalAttributes = map;

		RedisDataObject target = write(rand);

		assertThat(target.hashAsUtf8String().get("coworkers.[0].physicalAttributes.[hair-color]"), is("red"));
		assertThat(target.hashAsUtf8String().get("coworkers.[0].physicalAttributes.[eye-color]"), is("grey"));
	}

	@Test
	public void readSimpleMapValuesCorrectly() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("physicalAttributes.[hair-color]", "red");
		map.put("physicalAttributes.[eye-color]", "grey");

		RedisDataObject rdo = RedisDataObject.fromStringMap(map);

		Person target = converter.read(Person.class, rdo);

		assertThat(target.physicalAttributes, notNullValue());
		assertThat(target.physicalAttributes.get("hair-color"), is("red"));
		assertThat(target.physicalAttributes.get("eye-color"), is("grey"));
	}

	@Test
	public void writeAppendsMapWithComplexObjectsCorrectly() {

		Map<String, Person> map = new LinkedHashMap<String, Person>();
		Person janduin = new Person();
		janduin.firstname = "janduin";
		map.put("father", janduin);
		Person tam = new Person();
		tam.firstname = "tam";
		map.put("step-father", tam);

		rand.relatives = map;

		RedisDataObject target = write(rand);

		assertThat(target.hashAsUtf8String().get("relatives.[father].firstname"), is("janduin"));
		assertThat(target.hashAsUtf8String().get("relatives.[step-father].firstname"), is("tam"));
	}

	@Test
	public void readMapWithComplexObjectsCorrectly() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("relatives.[father].firstname", "janduin");
		map.put("relatives.[step-father].firstname", "tam");

		Person target = converter.read(Person.class, RedisDataObject.fromStringMap(map));

		assertThat(target.relatives, notNullValue());
		assertThat(target.relatives.get("father"), notNullValue());
		assertThat(target.relatives.get("father").firstname, is("janduin"));
		assertThat(target.relatives.get("step-father"), notNullValue());
		assertThat(target.relatives.get("step-father").firstname, is("tam"));
	}

	@Test
	public void writeAppendsClassTypeInformationCorrectlyForMapWithComplexObjects() {

		Map<String, Person> map = new LinkedHashMap<String, Person>();
		Person lews = new TaVeren();
		lews.firstname = "lews";
		map.put("previous-incarnation", lews);

		rand.relatives = map;

		RedisDataObject target = write(rand);

		assertThat(target.hashAsUtf8String().get("relatives.[previous-incarnation]._class"), is(TaVeren.class.getName()));
	}

	@Test
	public void readConsidersClassTypeInformationCorrectlyForMapWithComplexObjects() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("relatives.[previous-incarnation]._class", TaVeren.class.getName());
		map.put("relatives.[previous-incarnation].firstname", "lews");

		Person target = converter.read(Person.class, RedisDataObject.fromStringMap(map));

		assertThat(target.relatives.get("previous-incarnation"), notNullValue());
		assertThat(target.relatives.get("previous-incarnation"), instanceOf(TaVeren.class));
		assertThat(target.relatives.get("previous-incarnation").firstname, is("lews"));
	}

	private RedisDataObject write(Object source) {

		RedisDataObject rdo = new RedisDataObject();
		converter.write(source, rdo);
		return rdo;
	}

	@KeySpace("persons")
	static class Person {

		@Id String id;
		String firstname;

		List<String> nicknames;
		List<Person> coworkers;

		Address address;

		Map<String, String> physicalAttributes;
		Map<String, Person> relatives;
	}

	static class Address {

		String city;
		String country;
	}

	static class AddressWithPostcode extends Address {

		String postcode;
	}

	static class TaVeren extends Person {

	}

}
