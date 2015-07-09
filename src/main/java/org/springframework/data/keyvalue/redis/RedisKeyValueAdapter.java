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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.data.keyvalue.core.AbstractKeyValueAdapter;
import org.springframework.data.keyvalue.core.ForwardingCloseableIterator;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.BoundHashOperations;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.util.CloseableIterator;

/**
 * @author Christoph Strobl
 */
public class RedisKeyValueAdapter extends AbstractKeyValueAdapter {

	private RedisOperations<Serializable, Object> redisOps;

	public RedisKeyValueAdapter() {

		JedisConnectionFactory conFactory = new JedisConnectionFactory();
		conFactory.afterPropertiesSet();

		RedisTemplate<Serializable, Object> template = new RedisTemplate<Serializable, Object>();
		template.setConnectionFactory(conFactory);
		template.afterPropertiesSet();

		this.redisOps = template;
	}

	public RedisKeyValueAdapter(RedisOperations<Serializable, Object> redisOps) {
		this.redisOps = redisOps;
	}

	public Object put(Serializable id, Object item, Serializable keyspace) {

		BoundHashOperations<Serializable, Serializable, Object> ops = getHashOps(keyspace);
		ops.put(id, item);
		return item;
	}

	public boolean contains(Serializable id, Serializable keyspace) {
		return getHashOps(keyspace).hasKey(id);
	}

	public Object get(Serializable id, Serializable keyspace) {
		return getHashOps(keyspace).get(id);
	}

	public Object delete(Serializable id, Serializable keyspace) {

		Object o = get(id, keyspace);
		if (o != null) {
			getHashOps(keyspace).delete(id);
		}
		return o;
	}

	public List<?> getAllOf(Serializable keyspace) {
		return getHashOps(keyspace).values();
	}

	public void deleteAllOf(Serializable keyspace) {
		redisOps.delete(keyspace);
	}

	BoundHashOperations<Serializable, Serializable, Object> getHashOps(Serializable keyspace) {
		return redisOps.boundHashOps(keyspace);
	}

	public CloseableIterator<Entry<Serializable, Object>> entries(Serializable keyspace) {
		return new ForwardingCloseableIterator<Map.Entry<Serializable, Object>>(getHashOps(keyspace).entries().entrySet()
				.iterator());
	}

	public long count(Serializable keyspace) {
		return getHashOps(keyspace).size();
	}

	public void clear() {
		// TODO Auto-generated method stub
	}

	public void destroy() throws Exception {
		// TODO Auto-generated method stub
	}

}
