/*
 * Copyright 2002-2016 the original author or authors.
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

package com.dangdang.ddframe.reg.spring.annotation;

import com.dangdang.ddframe.job.api.ElasticJob;
import com.dangdang.ddframe.job.api.config.JobConfiguration;
import com.dangdang.ddframe.job.api.config.JobConfigurationFactory;
import com.dangdang.ddframe.job.api.config.impl.DataFlowJobConfiguration;
import com.dangdang.ddframe.job.api.config.impl.SimpleJobConfiguration;
import com.dangdang.ddframe.job.api.listener.ElasticJobListener;
import com.dangdang.ddframe.job.plugin.job.type.dataflow.AbstractBatchThroughputDataFlowElasticJob;
import com.dangdang.ddframe.job.plugin.job.type.dataflow.AbstractIndividualSequenceDataFlowElasticJob;
import com.dangdang.ddframe.job.plugin.job.type.simple.AbstractSimpleElasticJob;
import com.dangdang.ddframe.job.spring.schedule.SpringJobScheduler;
import com.dangdang.ddframe.reg.base.CoordinatorRegistryCenter;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.EmbeddedValueResolverAware;
import org.springframework.core.Ordered;
import org.springframework.util.StringUtils;
import org.springframework.util.StringValueResolver;

/**
 * Scheduled 注解处理器
 * @author: wesley.mail@qq.com
 */
public class ScheduledAnnotationProcessor implements BeanPostProcessor,
        Ordered, EmbeddedValueResolverAware, ApplicationContextAware {

	CoordinatorRegistryCenter regCenter;

	ElasticJobListener jobListener;

	StringValueResolver embeddedValueResolver;
	ApplicationContext applicationContext;

	@Override
	public int getOrder() {
		return LOWEST_PRECEDENCE;
	}

    @Autowired(required = false)
    public void setRegCenter(CoordinatorRegistryCenter regCenter) {
        this.regCenter = regCenter;
    }

    @Autowired(required = false)
    public void setJobListener(ElasticJobListener jobListener) {
        this.jobListener = jobListener;
    }

	@Override
	public void setEmbeddedValueResolver(StringValueResolver resolver) {
		this.embeddedValueResolver = resolver;
	}

	@Override
	public Object postProcessBeforeInitialization(Object bean, String beanName) {
		return bean;
	}

	@Override
	public Object postProcessAfterInitialization(final Object bean, String beanName) {
		Class<?> targetClass = AopUtils.getTargetClass(bean);

		if (!targetClass.isAnnotationPresent(Scheduled.class)){
			return bean;
		}
		if (!(bean instanceof ElasticJob)){
			return bean;
		}
        if (regCenter == null){
            throw new RuntimeException("A regCenter is required");
        }

		processScheduled(targetClass.getAnnotation(Scheduled.class), (ElasticJob)bean);
		return bean;
	}

	protected void processScheduled(Scheduled scheduled, ElasticJob job) {
		String cron = scheduled.cron();
		if (StringUtils.hasText(cron)) {
			if (this.embeddedValueResolver != null) {
				cron = this.embeddedValueResolver.resolveStringValue(cron);
			}
		}
		int shardingTotalCount = scheduled.shardingTotalCount();

		Class<? extends ElasticJob> jobClass = job.getClass();
		String jobName;
		if (!scheduled.name().isEmpty()) {
			jobName = scheduled.name();
		} else {
			jobName = jobClass.getSimpleName();
		}

		JobConfiguration jobConfig;
		if (job instanceof AbstractSimpleElasticJob) {
			final SimpleJobConfiguration simpleJobConfig = JobConfigurationFactory.createSimpleJobConfigurationBuilder(jobName,
					((AbstractSimpleElasticJob)job).getClass(), shardingTotalCount, cron).overwrite(scheduled.overwrite()).build();
			jobConfig = simpleJobConfig;
		}
		else if (job instanceof AbstractBatchThroughputDataFlowElasticJob) {
			final DataFlowJobConfiguration throughputJobConfig = JobConfigurationFactory.createDataFlowJobConfigurationBuilder(jobName,
					((AbstractBatchThroughputDataFlowElasticJob)job).getClass(), shardingTotalCount, cron).streamingProcess(true).build();
			jobConfig = throughputJobConfig;
		}
		else if (job instanceof AbstractIndividualSequenceDataFlowElasticJob) {
			final DataFlowJobConfiguration sequenceJobConfig = JobConfigurationFactory.createDataFlowJobConfigurationBuilder(jobName,
					((AbstractIndividualSequenceDataFlowElasticJob)job).getClass(), shardingTotalCount, cron).build();
			jobConfig = sequenceJobConfig;
		}
		else {
			return;
		}

		SpringJobScheduler jobScheduler;
        if (jobListener != null){
            jobScheduler = new SpringJobScheduler(regCenter, jobConfig, jobListener);
        }
        else {
            jobScheduler = new SpringJobScheduler(regCenter, jobConfig);
        }

		jobScheduler.setApplicationContext(applicationContext);
		jobScheduler.init();
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}
}
