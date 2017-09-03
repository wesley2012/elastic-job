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

import org.springframework.stereotype.Service;

import java.lang.annotation.*;

/**
 * 用于创建ElasticJob的注解
 * @author: wesley.mail@qq.com
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@Service
public @interface Scheduled {

    String cron() default "0 0 0 * * ?";
    String name() default "";
    int shardingTotalCount() default 1;
    boolean overwrite() default false;
}
