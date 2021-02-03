/*
 * Copyright 2017-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.repository.query;

import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.ReplaceOptions;
import com.couchbase.client.java.kv.UpsertOptions;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.manager.collection.ScopeSpec;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.search.SearchOptions;
import org.springframework.data.couchbase.core.support.MapList;
import org.springframework.http.server.DelegatingServerHttpResponse;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.util.ReactiveWrapperConverters;
import org.springframework.data.repository.util.ReactiveWrappers;

/**
 * Reactive {@link org.springframework.data.repository.query.ParametersParameterAccessor} implementation that subscribes
 * to reactive parameter wrapper types upon creation. This class performs synchronization when accessing parameters.
 *
 * @author Subhashni Balakrishnan
 * @author Mark Paluch
 * @since 3.0
 */
public class ReactiveCouchbaseParameterAccessor extends ParametersParameterAccessor {

	private final List<MonoProcessor<?>> subscriptions;
	private final MapList<Class<?>, Object> paramsOfType=new MapList();

	public ReactiveCouchbaseParameterAccessor(CouchbaseQueryMethod method, Object[] values) {
		super(method.getParameters(), values);
		this.subscriptions = new ArrayList<>(values.length);

		for (int i = 0; i < values.length; i++) {

			Object value = values[i];
			if(value != null){
				Object option = option (value);
				paramsOfType.putOne(option.getClass(), option);
			}

			if (value == null || !ReactiveWrappers.supports(value.getClass())) {
				subscriptions.add(null);
				continue;
			}

			if (ReactiveWrappers.isSingleValueType(value.getClass())) {
				subscriptions.add(ReactiveWrapperConverters.toWrapper(value, Mono.class).toProcessor());
			} else {
				subscriptions.add(ReactiveWrapperConverters.toWrapper(value, Flux.class).collectList().toProcessor());
			}
		}

	}

	private Object option(Object maybeOption) {
		Class<?> clazz = maybeOption.getClass();
		Class<?> componentClazz = clazz.getComponentType();
		if( componentClazz != null){
			Object[] optionArray = ((Object[])maybeOption);
			if(optionArray.length == 1){
				maybeOption = optionArray[0];
			} else if (CommonOptions.class.isAssignableFrom(componentClazz) || CollectionSpec.class.isAssignableFrom(componentClazz) || ScopeSpec.class.isAssignableFrom(componentClazz)){
				throw new IllegalArgumentException("there must be no more than one parameter of type "+clazz);
			} else {
				maybeOption = optionArray;
			}
		}
		return  maybeOption;

	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.ParametersParameterAccessor#getValue(int)
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected <T> T getValue(int index) {

		if (subscriptions.get(index) != null) {
			return (T) subscriptions.get(index).block();
		}

		return super.getValue(index);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.ParametersParameterAccessor#getBindableValue(int)
	 */
	public Object getBindableValue(int index) {
		return getValue(getParameters().getBindableParameter(index).getIndex());
	}

	public <T> List<T> getParamsOfType(Class<T> type) {
		List<T> result=new LinkedList();
		for(Class<?> t:paramsOfType.keySet()){
			if (type.isAssignableFrom(t)){
				result.add((T)t);
			}
		}
		return result;
	}

	public <T>  T getParamOfType(Class<T> type) {
		List<T> result = getParamsOfType(type);
		if(result.size() > 1){
			throw new IllegalArgumentException("there are multiple types of parameters that "+type+" isAssignableFrom"+result);
		}
		return result.size() == 1 ? (T)paramsOfType.getOne((Class<?>)result.get(0)) : null;
	}


}
