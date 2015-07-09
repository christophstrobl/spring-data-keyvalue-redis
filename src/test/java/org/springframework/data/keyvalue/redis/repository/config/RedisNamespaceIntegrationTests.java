package org.springframework.data.keyvalue.redis.repository.config;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionReader;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.keyvalue.core.mapping.KeyValuePersistentEntity;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.repository.support.Repositories;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Test class using the namespace configuration to set up the repository instance.
 * 
 * @author Mark Paluch
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class RedisNamespaceIntegrationTests {

	DefaultListableBeanFactory factory;
	BeanDefinitionReader reader;

	@Autowired ApplicationContext context;

	@Before
	public void setUp() throws InterruptedException {
		factory = new DefaultListableBeanFactory();
		reader = new XmlBeanDefinitionReader(factory);
	}

	@Test
	public void assertDefaultMappingContextIsWired() {

		reader.loadBeanDefinitions(new ClassPathResource("RedisNamespaceIntegrationTests-context.xml", getClass()));
		String s[] = factory.getBeanDefinitionNames();
		BeanDefinition definition = factory.getBeanDefinition("userRepository");
		assertThat(definition, is(notNullValue()));
	}

	@Test
	public void exposesPersistentEntity() {

		Repositories repositories = new Repositories(context);
		PersistentEntity<?, ?> entity = repositories.getPersistentEntity(UserRepository.User.class);
		assertThat(entity, is(notNullValue()));
		assertThat(entity, is(instanceOf(KeyValuePersistentEntity.class)));
	}
}
