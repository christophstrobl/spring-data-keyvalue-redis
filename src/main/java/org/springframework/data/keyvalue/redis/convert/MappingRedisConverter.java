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
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.springframework.core.CollectionFactory;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.ConverterFactory;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.convert.DefaultTypeMapper;
import org.springframework.data.convert.EntityInstantiator;
import org.springframework.data.convert.EntityInstantiators;
import org.springframework.data.convert.TypeAliasAccessor;
import org.springframework.data.convert.TypeMapper;
import org.springframework.data.keyvalue.core.mapping.KeyValuePersistentEntity;
import org.springframework.data.keyvalue.core.mapping.KeyValuePersistentProperty;
import org.springframework.data.keyvalue.core.mapping.context.KeyValueMappingContext;
import org.springframework.data.keyvalue.redis.Indexed;
import org.springframework.data.keyvalue.redis.Util.ByteUtils;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PreferredConstructor;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mapping.model.PersistentEntityParameterValueProvider;
import org.springframework.data.mapping.model.PropertyValueProvider;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.NumberUtils;

/**
 * {@link RedisConverter} implementation creating flat binary map structure out of a given domain type. Considers
 * {@link Indexed} annotation for enabling helper structures for finder operations.
 * 
 * @author Christoph Strobl
 */
public class MappingRedisConverter implements RedisConverter {

	static final Charset CHARSET = Charset.forName("UTF-8");

	private final KeyValueMappingContext mappingContext;
	private final GenericConversionService conversionService;
	private final EntityInstantiators entityInstantiators;
	private final TypeMapper<RedisDataObject> typeMapper;

	public MappingRedisConverter() {

		mappingContext = new KeyValueMappingContext();
		entityInstantiators = new EntityInstantiators();

		this.conversionService = new DefaultConversionService();
		this.conversionService.addConverter(new StringToBytesConverter());
		this.conversionService.addConverter(new BytesToStringConverter());
		this.conversionService.addConverter(new NumberToBytesConverter());
		this.conversionService.addConverterFactory(new BytesToNumberConverterFactory());
		// TODO: add converters for dates, and booleans

		typeMapper = new DefaultTypeMapper<RedisDataObject>(new RedisTypeAliasAccessor(this.conversionService));
	}

	public <R> R read(Class<R> type, final RedisDataObject source) {
		return readInternal("", type, source);
	}

	private <R> R readInternal(final String path, Class<R> type, final RedisDataObject source) {

		if (CollectionUtils.isEmpty(source.rawHash())) {
			return null;
		}

		TypeInformation<?> readType = typeMapper.readType(source);
		TypeInformation<?> typeToUse = readType != null ? readType : ClassTypeInformation.from(type);

		final KeyValuePersistentEntity<?> entity = mappingContext.getPersistentEntity(typeToUse);

		EntityInstantiator instantiator = entityInstantiators.getInstantiatorFor(entity);

		Object instance = instantiator.createInstance((KeyValuePersistentEntity) entity,
				new PersistentEntityParameterValueProvider<KeyValuePersistentProperty>(entity,
						new ConverterAwareParameterValueProvider(source, conversionService), null));

		final PersistentPropertyAccessor accessor = entity.getPropertyAccessor(instance);

		entity.doWithProperties(new PropertyHandler<KeyValuePersistentProperty>() {

			@Override
			public void doWithPersistentProperty(KeyValuePersistentProperty persistentProperty) {

				String currentPath = !path.isEmpty() ? path + "." + persistentProperty.getName() : persistentProperty.getName();

				PreferredConstructor<?, KeyValuePersistentProperty> constructor = entity.getPersistenceConstructor();

				if (constructor.isConstructorParameter(persistentProperty)) {
					return;
				}

				if (persistentProperty.isCollectionLike()) {

					Collection target = CollectionFactory.createCollection(persistentProperty.getType(),
							persistentProperty.getComponentType(), 10);

					if (conversionService.canConvert(byte[].class, persistentProperty.getComponentType())) {
						accessor.setProperty(
								persistentProperty,
								readCollection(currentPath, persistentProperty.getType(), source, persistentProperty
										.getTypeInformation().getComponentType()));
					} else {
						accessor.setProperty(
								persistentProperty,
								readCollectionOfComplexTypes(currentPath, persistentProperty.getType(), source, persistentProperty
										.getTypeInformation().getComponentType()));
					}

				} else if (persistentProperty.isEntity()) {

					Map<byte[], byte[]> raw = source.hashStartingWith(toBytes(currentPath + "."));

					Class<?> myType = persistentProperty.getTypeInformation().getActualType().getType();

					String type = fromBytes(raw.get(toBytes(currentPath + "._class")), String.class);
					if (type != null) {
						try {
							myType = ClassUtils.forName(type, null);
						} catch (ClassNotFoundException e) {
							throw new MappingException("Cannot find type " + type, e);
						} catch (LinkageError e) {
							throw new MappingException("Cannot find type " + type, e);
						}
					}

					accessor.setProperty(persistentProperty, readInternal(currentPath, myType, new RedisDataObject(raw)));

				} else {
					accessor.setProperty(persistentProperty,
							fromBytes(source.get(toBytes(currentPath)), persistentProperty.getActualType()));
				}
			}
		});

		return (R) instance;
	}

	public void write(Object source, final RedisDataObject sink) {

		final KeyValuePersistentEntity<?> entity = mappingContext.getPersistentEntity(source.getClass());
		final PersistentPropertyAccessor accessor = entity.getPropertyAccessor(source);

		typeMapper.writeType(ClassUtils.getUserClass(source), sink);
		sink.setKeyspace(toBytes(entity.getKeySpace()));

		entity.doWithProperties(new PropertyHandler<KeyValuePersistentProperty>() {

			@Override
			public void doWithPersistentProperty(KeyValuePersistentProperty persistentProperty) {

				if (persistentProperty.isIdProperty()) {

					sink.setId(toBytes(accessor.getProperty(persistentProperty)));
					sink.put(toBytes(persistentProperty.getName()), toBytes(accessor.getProperty(persistentProperty)));
					return;
				}

				if (persistentProperty.isCollectionLike()) {
					writeCollection(persistentProperty.getName(), (Collection<?>) accessor.getProperty(persistentProperty),
							persistentProperty.getTypeInformation().getComponentType(), sink);
				} else if (persistentProperty.isEntity()) {

					writePropertyInternal(persistentProperty.getName(), accessor.getProperty(persistentProperty),
							persistentProperty.getTypeInformation().getActualType(), sink);
				} else {

					if (persistentProperty.isAnnotationPresent(Indexed.class)) {
						sink.addIndex(toBytes(persistentProperty.getName() + ":" + accessor.getProperty(persistentProperty)));
					}

					sink.put(toBytes(persistentProperty.getName()), toBytes(accessor.getProperty(persistentProperty)));
				}
			}
		});

	}

	private void writePropertyInternal(final String path, final Object value, TypeInformation<?> typeHint,
			final RedisDataObject sink) {

		if (value == null) {
			return;
		}

		if (value.getClass() != typeHint.getType()) {
			sink.put(toBytes(path + "._class"), toBytes(value.getClass().getName()));
		}

		final KeyValuePersistentEntity<?> entity = mappingContext.getPersistentEntity(value.getClass());
		final PersistentPropertyAccessor accessor = entity.getPropertyAccessor(value);

		entity.doWithProperties(new PropertyHandler<KeyValuePersistentProperty>() {

			@Override
			public void doWithPersistentProperty(KeyValuePersistentProperty persistentProperty) {

				String propertyStringPath = path + "." + persistentProperty.getName();

				if (persistentProperty.isCollectionLike()) {
					writeCollection(propertyStringPath, (Collection<?>) accessor.getProperty(persistentProperty),
							persistentProperty.getTypeInformation().getComponentType(), sink);
				} else if (persistentProperty.isEntity()) {
					writePropertyInternal(propertyStringPath, accessor.getProperty(persistentProperty), persistentProperty
							.getTypeInformation().getActualType(), sink);
				} else {
					sink.put(toBytes(propertyStringPath), toBytes(accessor.getProperty(persistentProperty)));
				}
			}
		});

	}

	private void writeCollection(String path, Collection<?> values, TypeInformation<?> typeHint, RedisDataObject sink) {

		if (values == null) {
			return;
		}

		int i = 0;
		for (Object o : values) {

			String currentPath = path + ".[" + i + "]";

			if (conversionService.canConvert(o.getClass(), byte[].class)) {
				sink.put(toBytes(currentPath), toBytes(o));
			} else {
				writePropertyInternal(currentPath, o, typeHint, sink);
			}
			i++;
		}
	}

	private Collection<?> readCollectionOfComplexTypes(String path, Class<?> collectionType, RedisDataObject rdo,
			TypeInformation<?> typeHint) {

		Collection target = CollectionFactory.createCollection(collectionType, typeHint.getActualType().getType(), 10);

		Set<byte[]> values = rdo.keyRange(toBytes(path + ".["));

		for (byte[] value : values) {

			System.out.println(new String(value));

			RedisDataObject source = new RedisDataObject(rdo.hashStartingWith(value));

			System.out.println(source);
			Object o = readInternal(fromBytes(value, String.class), typeHint.getActualType().getType(), source);

			System.out.println(o);
			target.add(o);
		}
		return target;
	}

	private Collection<?> readCollection(String path, Class<?> collectionType, RedisDataObject rdo,
			TypeInformation<?> typeHint) {

		Collection target = CollectionFactory.createCollection(collectionType, typeHint.getActualType().getType(), 10);

		Map<byte[], byte[]> values = rdo.hashStartingWith(toBytes(path + ".["));

		for (byte[] value : values.values()) {
			target.add(fromBytes(value, typeHint.getType()));
		}
		return target;
	}

	public byte[] toBytes(Object source) {

		if (source instanceof byte[]) {
			return (byte[]) source;
		}

		return conversionService.convert(source, byte[].class);
	}

	public <T> T fromBytes(byte[] source, Class<T> type) {
		return conversionService.convert(source, type);
	}

	public byte[] byteEntityId(Object keyspace, Object id) {
		return ByteUtils.concatAll(toBytes(keyspace), RedisDataObject.ID_SEPERATOR, toBytes(id));
	}

	public byte[] indexId(Object keyspace, Object path) {

		byte[] bytePath = toBytes(path);
		byte[] keyspaceBin = toBytes(keyspace);

		return ByteUtils.concatAll(keyspaceBin, RedisDataObject.PATH_SEPERATOR, bytePath);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.convert.EntityConverter#getMappingContext()
	 */
	public MappingContext<? extends KeyValuePersistentEntity<?>, KeyValuePersistentProperty> getMappingContext() {
		return this.mappingContext;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.convert.EntityConverter#getConversionService()
	 */
	public ConversionService getConversionService() {
		return this.conversionService;
	}

	private static class ConverterAwareParameterValueProvider implements
			PropertyValueProvider<KeyValuePersistentProperty> {

		private final RedisDataObject source;
		private final ConversionService conversionService;

		public ConverterAwareParameterValueProvider(RedisDataObject source, ConversionService conversionService) {
			this.source = source;
			this.conversionService = conversionService;
		}

		@Override
		public <T> T getPropertyValue(KeyValuePersistentProperty property) {
			return (T) conversionService.convert(source.get(conversionService.convert(property.getName(), byte[].class)),
					property.getActualType());
		}

	}

	private static class RedisTypeAliasAccessor implements TypeAliasAccessor<RedisDataObject> {

		private final byte[] typeKey;

		private final ConversionService conversionService;

		RedisTypeAliasAccessor(ConversionService conversionService) {
			this(conversionService, "_class");
		}

		RedisTypeAliasAccessor(ConversionService conversionService, String typeKey) {

			this.conversionService = conversionService;
			this.typeKey = conversionService.convert(typeKey, byte[].class);
		}

		@Override
		public Object readAliasFrom(RedisDataObject source) {
			return conversionService.convert(source.get(typeKey), String.class);
		}

		@Override
		public void writeTypeTo(RedisDataObject sink, Object alias) {
			sink.put(typeKey, conversionService.convert(alias, byte[].class));
		}

	}

	private static class StringBasedConverter {

		byte[] fromString(String source) {

			if (source == null) {
				return new byte[] {};
			}

			return source.getBytes(CHARSET);
		}

		String toString(byte[] source) {
			return new String(source, CHARSET);
		}
	}

	private static class StringToBytesConverter extends StringBasedConverter implements Converter<String, byte[]> {

		@Override
		public byte[] convert(String source) {
			return fromString(source);
		}

	}

	private static class BytesToStringConverter extends StringBasedConverter implements Converter<byte[], String> {

		@Override
		public String convert(byte[] source) {
			return toString(source);
		}

	}

	private static class NumberToBytesConverter extends StringBasedConverter implements Converter<Number, byte[]> {

		@Override
		public byte[] convert(Number source) {

			if (source == null) {
				return new byte[] {};
			}

			return fromString(source.toString());
		}
	}

	private static class BytesToNumberConverterFactory implements ConverterFactory<byte[], Number> {

		@Override
		public <T extends Number> Converter<byte[], T> getConverter(Class<T> targetType) {
			return new BytesToNumberConverter<T>(targetType);
		}

		private static final class BytesToNumberConverter<T extends Number> extends StringBasedConverter implements
				Converter<byte[], T> {

			private final Class<T> targetType;

			public BytesToNumberConverter(Class<T> targetType) {
				this.targetType = targetType;
			}

			@Override
			public T convert(byte[] source) {

				if (source == null || source.length == 0) {
					return null;
				}

				return NumberUtils.parseNumber(toString(source), targetType);
			}
		}

	}

}
