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

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

public class RedisOperationChain {

	Set<Object> sismember = new LinkedHashSet<Object>();
	Set<Object> orSismember = new LinkedHashSet<Object>();

	public void sismember(Object next) {
		sismember.add(next);
	}

	public Set<Object> getSismember() {
		return sismember;
	}

	public void orSismember(Object next) {
		orSismember.add(next);
	}

	public void orSismember(Collection<Object> next) {
		orSismember.addAll(next);
	}

	public Set<Object> getOrSismember() {
		return orSismember;
	}

}
