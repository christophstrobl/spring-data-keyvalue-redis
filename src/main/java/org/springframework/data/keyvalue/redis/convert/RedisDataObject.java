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
package org.springframework.data.keyvalue.redis.convert;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.springframework.data.keyvalue.redis.Util.ByteUtils;

/**
 * Data object holding flat hash values, to be stored in redis hash, representing the domain object. Index information
 * points to additional structures holding the objects is for searching.
 * 
 * @author Christoph Strobl
 */
public class RedisDataObject {

	private byte[] keyspace;
	private byte[] id;
	public static final byte[] ID_SEPERATOR = ":".getBytes(Charset.forName("UTF-8"));
	public static final byte[] PATH_SEPERATOR = ".".getBytes(Charset.forName("UTF-8"));

	private Map<ByteArrayWrapper, byte[]> hash;
	private Set<ByteArrayWrapper> index;

	public RedisDataObject() {
		this.hash = new LinkedHashMap<ByteArrayWrapper, byte[]>();
		this.index = new HashSet<ByteArrayWrapper>();
	}

	public RedisDataObject(Map<byte[], byte[]> raw) {
		this();

		for (Entry<byte[], byte[]> entry : raw.entrySet()) {
			put(entry.getKey(), entry.getValue());
		}
	}

	public static RedisDataObject fromStringMap(Map<String, String> source) {

		RedisDataObject rdo = new RedisDataObject();
		if (source == null) {
			return rdo;
		}

		for (Entry<String, String> entry : source.entrySet()) {
			rdo.put(entry.getKey().getBytes(Charset.forName("UTF-8")), entry.getValue().getBytes(Charset.forName("UTF-8")));
		}
		return rdo;
	}

	public void put(byte[] key, byte[] value) {

		if (hasValue(key) && hasValue(value)) {
			hash.put(new ByteArrayWrapper(key), value);
		}
	}

	public byte[] getKey() {
		return ByteUtils.concatAll(keyspace != null ? keyspace : new byte[] {}, ID_SEPERATOR, id != null ? id
				: new byte[] {});
	}

	public void setId(byte[] id) {
		this.id = id;
	}

	public byte[] getId() {
		return this.id;
	}

	public byte[] get(byte[] key) {
		return hash.get(new ByteArrayWrapper(key));
	}

	private boolean hasValue(byte[] source) {
		return source != null && source.length != 0;
	}

	@Override
	public String toString() {
		return "RedisDataObject [key=" + keyAsUtf8String() + ", hash=" + hashAsUtf8String() + "]";
	}

	public Map<byte[], byte[]> rawHash() {

		Map<byte[], byte[]> map = new LinkedHashMap<byte[], byte[]>();
		for (Entry<ByteArrayWrapper, byte[]> entry : hash.entrySet()) {
			map.put(entry.getKey().getData(), entry.getValue());
		}
		return map;
	}

	public Map<byte[], byte[]> hashStartingWith(byte[] prefix) {

		Map<byte[], byte[]> map = new LinkedHashMap<byte[], byte[]>();
		for (Entry<ByteArrayWrapper, byte[]> entry : hash.entrySet()) {

			if (entry.getKey().startsWith(prefix)) {
				map.put(entry.getKey().getData(), entry.getValue());
			}
		}
		return map;
	}

	public Set<byte[]> keyRange(byte[] prefix) {

		Set<ByteArrayWrapper> keys = new LinkedHashSet<ByteArrayWrapper>();

		for (Entry<byte[], byte[]> entry : hashStartingWith(prefix).entrySet()) {

			for (int i = prefix.length; i < entry.getKey().length; i++) {
				if (entry.getKey()[i] == ']') {
					keys.add(new ByteArrayWrapper(Arrays.copyOfRange(entry.getKey(), 0, i + 1)));
					break;
				}
			}
		}

		Set<byte[]> result = new LinkedHashSet<byte[]>();
		for (ByteArrayWrapper wrapper : keys) {
			result.add(wrapper.getData());
		}
		return result;
	}

	public Map<String, String> hashAsUtf8String() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		for (Entry<ByteArrayWrapper, byte[]> entry : hash.entrySet()) {
			map.put(new String(entry.getKey().getData(), Charset.forName("UTF-8")),
					new String(entry.getValue(), Charset.forName("UTF-8")));
		}
		return map;
	}

	public String keyAsUtf8String() {

		if (getKey() == null) {
			return null;
		}
		return new String(getKey(), Charset.forName("UTF-8"));
	}

	static class ByteArrayWrapper {

		private final byte[] data;

		public ByteArrayWrapper(byte[] data) {
			if (data == null) {
				throw new NullPointerException();
			}
			this.data = data;
		}

		byte[] getData() {
			return data;
		}

		@Override
		public boolean equals(Object other) {
			if (!(other instanceof ByteArrayWrapper)) {
				return false;
			}
			return Arrays.equals(data, ((ByteArrayWrapper) other).data);
		}

		@Override
		public int hashCode() {
			return Arrays.hashCode(data);
		}

		public boolean startsWith(byte[] prefix) {

			if (prefix.length > data.length) {
				return false;
			}

			for (int i = 0; i < prefix.length; i++) {
				if (data[i] != prefix[i]) {
					return false;
				}
			}
			return true;
		}
	}

	public void addIndex(byte[] bytes) {
		this.index.add(new ByteArrayWrapper(bytes));
	}

	public Set<byte[]> getIndexKeys() {

		Set<byte[]> target = new HashSet<byte[]>();
		for (ByteArrayWrapper wrapper : this.index) {
			target.add(ByteUtils.concatAll(keyspace, PATH_SEPERATOR, wrapper.getData()));
		}
		return target;
	}

	public byte[] getKeyspace() {
		return keyspace;
	}

	public void setKeyspace(byte[] keyspace) {
		this.keyspace = keyspace;
	}

}
