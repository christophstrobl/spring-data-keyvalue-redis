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
package org.springframework.data.keyvalue.redis.repository.query;

import java.util.Iterator;

import org.springframework.data.domain.Sort;
import org.springframework.data.keyvalue.core.query.KeyValueQuery;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;

public class RedisQueryCreator extends AbstractQueryCreator<KeyValueQuery<RedisOperationChain>, RedisOperationChain> {

	public RedisQueryCreator(PartTree tree, ParameterAccessor parameters) {

		super(tree, parameters);

	}

	@Override
	protected RedisOperationChain create(Part part, Iterator<Object> iterator) {
		return from(part, iterator, new RedisOperationChain());
	}

	private RedisOperationChain from(Part part, Iterator<Object> iterator, RedisOperationChain sink) {

		switch (part.getType()) {
			case SIMPLE_PROPERTY:
				sink.sismember(part.getProperty().toDotPath() + ":" + iterator.next());
				break;
			default:
				throw new IllegalArgumentException(part.getType() + "is not supported for redis query derivation");
		}

		return sink;

	}

	@Override
	protected RedisOperationChain and(Part part, RedisOperationChain base, Iterator<Object> iterator) {
		return from(part, iterator, base);
	}

	@Override
	protected RedisOperationChain or(RedisOperationChain base, RedisOperationChain criteria) {
		base.orSismember(criteria.sismember);
		return base;
	}

	@Override
	protected KeyValueQuery<RedisOperationChain> complete(final RedisOperationChain criteria, Sort sort) {

		KeyValueQuery<RedisOperationChain> query = new KeyValueQuery<RedisOperationChain>(criteria);

		if (sort != null) {
			query.setSort(sort);
		}

		return query;
	}

}
