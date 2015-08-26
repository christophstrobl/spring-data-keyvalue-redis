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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.springframework.dao.DataAccessException;
import org.springframework.data.keyvalue.core.AbstractKeyValueAdapter;
import org.springframework.data.keyvalue.redis.convert.MappingRedisConverter;
import org.springframework.data.keyvalue.redis.convert.RedisConverter;
import org.springframework.data.keyvalue.redis.convert.RedisDataObject;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.BoundHashOperations;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.util.CloseableIterator;

/**
 * @author Christoph Strobl
 */
public class RedisKeyValueAdapter extends AbstractKeyValueAdapter {

	private RedisOperations<byte[], byte[]> redisOps;

	private MappingRedisConverter converter;

	public RedisKeyValueAdapter() {

		super(new RedisQueryEngine());

		converter = new MappingRedisConverter();

		JedisConnectionFactory conFactory = new JedisConnectionFactory();
		conFactory.afterPropertiesSet();

		RedisTemplate<byte[], byte[]> template = new RedisTemplate<byte[], byte[]>();
		template.setConnectionFactory(conFactory);
		template.afterPropertiesSet();

		this.redisOps = template;
	}

	public RedisKeyValueAdapter(RedisOperations<byte[], byte[]> redisOps) {
		this.redisOps = redisOps;
	}

	public Object put(final Serializable id, final Object item, final Serializable keyspace) {

		final RedisDataObject rdo = new RedisDataObject();
		converter.write(item, rdo);

		redisOps.execute(new RedisCallback<Object>() {

			@Override
			public Object doInRedis(RedisConnection connection) throws DataAccessException {

				connection.hMSet(rdo.getKey(), rdo.rawHash());
				connection.sAdd(rdo.getKeyspace(), rdo.getId());

				if (!rdo.getIndexKeys().isEmpty()) {
					for (byte[] index : rdo.getIndexKeys()) {
						connection.sAdd(index, rdo.getId());
					}
				}
				return null;
			}
		});

		return item;
	}

	public boolean contains(final Serializable id, final Serializable keyspace) {

		Boolean exists = redisOps.execute(new RedisCallback<Boolean>() {

			@Override
			public Boolean doInRedis(RedisConnection connection) throws DataAccessException {
				return connection.sIsMember(converter.toBytes(keyspace), converter.toBytes(id));
			}
		});

		return exists != null ? exists.booleanValue() : false;
	}

	public Object get(Serializable id, Serializable keyspace) {

		final byte[] binId = converter.byteEntityId(keyspace, id);

		Map<byte[], byte[]> raw = redisOps.execute(new RedisCallback<Map<byte[], byte[]>>() {

			@Override
			public Map<byte[], byte[]> doInRedis(RedisConnection connection) throws DataAccessException {
				return connection.hGetAll(binId);
			}
		});

		return converter.read(Object.class, new RedisDataObject(raw));
	}

	public Object delete(final Serializable id, final Serializable keyspace) {

		final byte[] binId = converter.toBytes(id);
		final byte[] binKeyspace = converter.toBytes(keyspace);

		Object o = get(id, keyspace);

		if (o != null) {

			redisOps.execute(new RedisCallback<Void>() {

				@Override
				public Void doInRedis(RedisConnection connection) throws DataAccessException {

					connection.del(converter.byteEntityId(binKeyspace, binId));
					connection.sRem(binKeyspace, binId);

					Set<byte[]> potentialIndex = connection.keys(converter.toBytes(keyspace + ".*"));

					for (byte[] indexKey : potentialIndex) {
						try {
							connection.sRem(indexKey, binId);
						} catch (Exception e) {
							System.err.println(e);
						}
					}
					return null;
				}
			});

		}
		return o;
	}

	public List<?> getAllOf(final Serializable keyspace) {

		final byte[] binKeyspace = converter.toBytes(keyspace);

		List<Map<byte[], byte[]>> raw = redisOps.execute(new RedisCallback<List<Map<byte[], byte[]>>>() {

			@Override
			public List<Map<byte[], byte[]>> doInRedis(RedisConnection connection) throws DataAccessException {

				final List<Map<byte[], byte[]>> rawData = new ArrayList<Map<byte[], byte[]>>();

				Set<byte[]> members = connection.sMembers(binKeyspace);

				for (byte[] id : members) {
					rawData.add(connection.hGetAll(converter.byteEntityId(binKeyspace, id)));
				}

				return rawData;
			}
		});

		List<Object> result = new ArrayList<Object>(raw.size());
		for (Map<byte[], byte[]> rawData : raw) {
			result.add(converter.read(Object.class, new RedisDataObject(rawData)));
		}

		return result;
	}

	public void deleteAllOf(final Serializable keyspace) {

		redisOps.execute(new RedisCallback<Void>() {

			@Override
			public Void doInRedis(RedisConnection connection) throws DataAccessException {

				connection.del(converter.toBytes(keyspace));

				Set<byte[]> potentialIndex = connection.keys(converter.toBytes(keyspace + ".*"));

				for (byte[] indexKey : potentialIndex) {
					connection.del(indexKey);
				}
				return null;
			}
		});
	}

	BoundHashOperations<byte[], byte[], Object> getHashOps(Serializable keyspace) {
		return redisOps.boundHashOps(converter.getConversionService().convert(keyspace, byte[].class));
	}

	public CloseableIterator<Entry<Serializable, Object>> entries(Serializable keyspace) {
		throw new UnsupportedOperationException("Not yet implemented");
	}

	public long count(final Serializable keyspace) {

		Long count = redisOps.execute(new RedisCallback<Long>() {

			@Override
			public Long doInRedis(RedisConnection connection) throws DataAccessException {
				return connection.sCard(converter.toBytes(keyspace));
			}
		});

		return count != null ? count.longValue() : 0;
	}

	public <T> T execute(RedisCallback<T> callback) {
		return redisOps.execute(callback);
	}

	public RedisConverter getConverter() {
		return this.converter;
	}

	public void clear() {
		// TODO Auto-generated method stub
	}

	public void destroy() throws Exception {
		// TODO Auto-generated method stub
	}

}
