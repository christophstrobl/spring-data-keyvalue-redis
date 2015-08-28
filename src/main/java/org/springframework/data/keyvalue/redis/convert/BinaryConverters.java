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
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;

import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.ConverterFactory;
import org.springframework.util.NumberUtils;

/**
 * @author Christoph Strobl
 */
final class BinaryConverters {

	/**
	 * Use {@literal UTF-8} as default charset.
	 */
	public static final Charset CHARSET = Charset.forName("UTF-8");

	private BinaryConverters() {}

	/**
	 * @author Christoph Strobl
	 */
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

	/**
	 * @author Christoph Strobl
	 */
	static class StringToBytesConverter extends StringBasedConverter implements Converter<String, byte[]> {

		@Override
		public byte[] convert(String source) {
			return fromString(source);
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	static class BytesToStringConverter extends StringBasedConverter implements Converter<byte[], String> {

		@Override
		public String convert(byte[] source) {
			return toString(source);
		}

	}

	/**
	 * @author Christoph Strobl
	 */
	static class NumberToBytesConverter extends StringBasedConverter implements Converter<Number, byte[]> {

		@Override
		public byte[] convert(Number source) {

			if (source == null) {
				return new byte[] {};
			}

			return fromString(source.toString());
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	static class EnumToBytesConverter extends StringBasedConverter implements Converter<Enum<?>, byte[]> {

		@Override
		public byte[] convert(Enum<?> source) {

			if (source == null) {
				return new byte[] {};
			}

			return fromString(source.toString());
		}

	}

	/**
	 * @author Christoph Strobl
	 */
	static final class BytesToEnumConverterFactory implements ConverterFactory<byte[], Enum<?>> {

		@Override
		@SuppressWarnings({ "unchecked", "rawtypes" })
		public <T extends Enum<?>> Converter<byte[], T> getConverter(Class<T> targetType) {

			Class<?> enumType = targetType;
			while (enumType != null && !enumType.isEnum()) {
				enumType = enumType.getSuperclass();
			}
			if (enumType == null) {
				throw new IllegalArgumentException("The target type " + targetType.getName() + " does not refer to an enum");
			}
			return new BytesToEnum(enumType);
		}

		/**
		 * @author Christoph Strobl
		 */
		private class BytesToEnum<T extends Enum<T>> extends StringBasedConverter implements Converter<byte[], T> {

			private final Class<T> enumType;

			public BytesToEnum(Class<T> enumType) {
				this.enumType = enumType;
			}

			@Override
			public T convert(byte[] source) {

				String value = toString(source);

				if (value == null || value.length() == 0) {
					return null;
				}
				return (T) Enum.valueOf(this.enumType, value.trim());
			}
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	static class BytesToNumberConverterFactory implements ConverterFactory<byte[], Number> {

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

	/**
	 * @author Christoph Strobl
	 */
	static class BooleanToBytesConverter extends StringBasedConverter implements Converter<Boolean, byte[]> {

		final byte[] _true = fromString("1");
		final byte[] _false = fromString("0");

		@Override
		public byte[] convert(Boolean source) {

			if (source == null) {
				return new byte[] {};
			}

			return source.booleanValue() ? _true : _false;
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	static class BytesToBooleanConverter extends StringBasedConverter implements Converter<byte[], Boolean> {

		@Override
		public Boolean convert(byte[] source) {

			if (source == null || source.length == 0) {
				return null;
			}

			String value = toString(source);
			return ("1".equals(value) || "true".equalsIgnoreCase(value)) ? Boolean.TRUE : Boolean.FALSE;
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	static class DateToBytesConverter extends StringBasedConverter implements Converter<Date, byte[]> {

		@Override
		public byte[] convert(Date source) {

			if (source == null) {
				return new byte[] {};
			}

			return fromString(Long.toString(source.getTime()));
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	static class BytesToDateConverter extends StringBasedConverter implements Converter<byte[], Date> {

		@Override
		public Date convert(byte[] source) {

			if (source == null || source.length == 0) {
				return null;
			}

			String value = toString(source);
			try {
				return new Date(NumberUtils.parseNumber(value, Long.class));
			} catch (NumberFormatException nfe) {
				// ignore
			}

			try {
				return DateFormat.getInstance().parse(value);
			} catch (ParseException e) {
				// ignore
			}

			throw new IllegalArgumentException("Cannot parse date out of " + source);
		}
	}
}
