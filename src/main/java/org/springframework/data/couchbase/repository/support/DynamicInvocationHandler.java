package org.springframework.data.couchbase.repository.support;

import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.ReplaceOptions;
import com.couchbase.client.java.kv.UpsertOptions;
import com.couchbase.client.java.query.QueryOptions;
import org.springframework.data.couchbase.core.support.CollectionName;
import org.springframework.data.couchbase.core.support.MapList;
import org.springframework.data.couchbase.core.support.ScopeName;
import org.springframework.data.couchbase.repository.CouchbaseRepository;
import org.springframework.data.couchbase.repository.query.CouchbaseEntityInformation;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class DynamicInvocationHandler<T1 extends CouchbaseRepository> implements InvocationHandler {
	T1 target;
	CommonOptions<?> options;
	CollectionName collection;
	ScopeName scope;
	Class<?> repositoryClass;
	CouchbaseEntityInformation entityInformation;
	MapList<String, Class<?>> optionsMap = new MapList<>();

	public DynamicInvocationHandler(T1 target, Class<?> repositoryClass, CommonOptions<?> options, String collection,
			String scope) {
		this.target = target;
		this.entityInformation = target.getEntityInformation();
		this.options = options;
		this.collection = collection == null ? null : new CollectionName(collection);
		this.scope = scope == null ? null : new ScopeName(scope);
		this.repositoryClass = repositoryClass;
		this.optionsMap = new MapList<>();
		for (Method m : repositoryClass.getMethods()) {
			if (optionsMap.get(m.getName()) == null) {
				if (m.getName().startsWith("save")) {
					this.optionsMap.put(m.getName(), Arrays.asList(UpsertOptions.class, ReplaceOptions.class));
				} else if (m.getName().startsWith("delete")) {
					this.optionsMap.putOne(m.getName(), RemoveOptions.class);
				} else {
					this.optionsMap.putOne(m.getName(), QueryOptions.class);
				}
			}
		}
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

		if (method.getName().equals("toString")) {
			return proxy.getClass() + " " + target.toString();
		}

		if (method.getName().equals("withOptions")) {
			options = (CommonOptions<?>) args[0];
			return proxy;
		}
		if (method.getName().equals("withScope")) {
			scope = new ScopeName((String) args[0]);
			return proxy;
		}
		if (method.getName().equals("withCollection")) {
			collection = new CollectionName((String) args[0]);
			return proxy;
		}
		// if last arg is var-args CommonOptions array, then change it to CommonOptions object before adding other args
		// because other signatures have CommonOptions object signature (not var-args)
		Class<?> lastArgComponentType = args != null && args.length > 0 && args[args.length - 1] != null
				? args[args.length - 1].getClass().getComponentType()
				: null;
		if (lastArgComponentType != null && CommonOptions.class.isAssignableFrom(lastArgComponentType)) {
			args[args.length - 1] = ((Object[]) (args[args.length - 1]))[0];
		}

		/*
			On the proxy, the caller may have set/not-set any combination of Options, Collection, Scope with the only restriction being
			 that if scope is set, collection must also be set (a non-default scope does not have a default collection)
			on the proxy.
			The trailing args can also be any of that combination.
			The two combinations must be merged and the result applied to the args to lookup the method and make the call.
			For now, let's have the args override the values on the proxy.
		
			1) options that are not Options, CollectionName, ScopeName are left untouched ("Brubaker")
			2) The remaining arguments, if present, will be used as-is.
			3) And if not present, must be added (or inserted), in order, with the value from the proxy.
		
			Their are six allowable forms of each call.
		
			findByLastname( lastname )
			findByLastname( lastname, Options )
			findByLastname( lastname, Options, collection)
			findByLastname( lastname, Options, collection, scope)
			findByLastname( lastname, collection )
			findByLastname( lastname, collection, scope )
		
			To reduce the number of definitions, varargs signatures can be used
		
			findByLastname( lastname, Options... )
			findByLastname( lastname, Options, collection, scope...)
			findByLastname( lastname, collection, scope... )
		
			The twist is that the last arg is always an array that contains the arg.
			
			To determine which arg is which, we examine the class of the argument. 
			Since arguments with null values do not have a class, we will assume they are not Options, Collection or Scope.
			So the general procedure will be 
			1) take all the args up to the first Option, Collection or Scope arg. make this baseArgs.
			2) if there is a Option arg, leave it as is.  If the proxy also has option != null, throw an exception.
			3) If there is no Option arg, but the proxy has an option, then add that on to the end of the baseArgs
			4) If there is no Option arg, and the proxy does not have an option, then add a null arg to the end of 
			baseArgs.(this is same as (3))
			5) Repeat for collection and scope.
		
			QueryOptions proxyOptions=QueryOptions.createOptions().scanConsistency(NOT_BOUNDED);
			QueryOptions argsOptions=QueryOptions.createOptions().scanConsistency(REQUEST_PLUS);
			myRepository.withOptions(proxyOptions).findByLastName("Brubaker", argsOptions);
		
			method:"findByLastname"
			proxy{ Options:scan=REQUEST_PLUS, collection:null, scope:null}
			args[ "Brubaker", Options:scan=NOT_BOUNDED ]
		
			This should result in the call
			findByLastname( "Brubaker", Options:scan=REQUEST_PLUS, collection:null, scope:null);
		
		 */

		// if options are specified for a method that does not take Options (save() etc.)
		// there will be NoSuchMethodError thrown

		List<Object> baseArgs = new LinkedList<>();
		int i = 0;
		// process the args not related to CommonOptions, CollectionName, ScopeName
		i = doBaseArgs(baseArgs, args, i);
		// process the CommonOptions, CollectionName, ScopeName
		// If they are already args, just pas them along.
		// If they are not in args, insert the values from the proxy into args
		i = doNextArg(baseArgs, args, i, CommonOptions.class, options);
		i = doNextArg(baseArgs, args, i, CollectionName.class, collection);
		i = doNextArg(baseArgs, args, i, ScopeName[].class, new ScopeName[] { scope }); // scopeName is varargs

		args = baseArgs.toArray();
		Class<?>[] paramTypes = Arrays.stream(args).map(o -> o == null ? null : o.getClass()).toArray(Class<?>[]::new);

		Method theMethod = null;
		List<Exception> ee = new LinkedList<>();
		// just one try here for now. If multiple tries are necessary, try with orig params, and if that fails,
		// then try Object.class or Iterable.class. But I think one try is ok.
		for (int tries = 0; tries < 1; tries++) {
			if (args.length > 0 && args[0] != null) {
				if (args[0].getClass().equals(entityInformation.getJavaType())) {
					paramTypes[0] = Object.class;
				} else if (Iterable.class.isAssignableFrom(args[0].getClass())) {
					paramTypes[0] = Iterable.class;
				} else {
					; // leave as-is
				}
			}
			for (Class<?> optionsClass : optionsMap.get(method.getName())) { // save could be Upsert or Replace
				try {
					// If the methods are not defined in the Repository interface (airportRepository etc),
					// the method signature will be from CouchbaseRepository and
					// will have the entity parameter with type Object
					// instead of Airport etc and will only be resolved on the second try

					paramTypes[paramTypes.length - 3] = optionsClass;
					System.out.println("   method  " + debugMethod(method.getName(), paramTypes));
					theMethod = repositoryClass.getMethod(method.getName(), paramTypes);
					System.out.println("theMethod  " + debugMethod(theMethod));
					break;
				} catch (NoSuchMethodException e) {
					ee.add(e);
				}
			}
			if (theMethod != null)
				break;
		}

		if (theMethod == null) {
			String description = ee.stream().map(Exception::toString).collect(Collectors.joining("\n"));
			throw new NoSuchMethodException("Did not find any repository method \n" + description);
		}
		Object result = theMethod.invoke(target, args);
		return result;
	}

	/**
	 * copy all the args that are not CommonOptions, CollectionName, ScopeName into baseArgs Stop when we get to (the
	 * first) CommonOptions, CollectionName, ScopeName <br>
	 * TODO: question. What happens when the first (non-null) arg of one of these types is not CommonOptions? Suppose it
	 * is CollectionName as in a call like save( entity, null, collectionName, scopename) That means that the previous arg
	 * was CommonOptions even though we did not recognize it as such (and therefore we will insert one before inserting
	 * CollectionName). Hopefully a customer would not do that.
	 * 
	 * @param baseArgs
	 * @param args
	 * @param i
	 * @return
	 */
	private int doBaseArgs(List<Object> baseArgs, Object[] args, int i) {
		if (args == null) {
			return 0;
		}
		while (i < args.length) {
			if (args[i] == null) { // we don't no what a null arg is, just run it through.
				baseArgs.add(null);
			} else {
				Class<?> argClass = args[i].getClass().isArray() ? args[i].getClass().getComponentType() : args[i].getClass();
				if (!(CommonOptions.class.isAssignableFrom(argClass) || CollectionName.class.isAssignableFrom(argClass)
						|| ScopeName.class.isAssignableFrom(argClass))) {
					baseArgs.add(args[i]);
				} else {
					if (!CommonOptions.class.isAssignableFrom(argClass) && i > 0 && args[i - 1] == null) {
						// back up to the empty spot
						i--;
						// put an empty options arg there. we should get the type of this option from optionsMap
						baseArgs.remove(i); // remove the null. doNextArg will add CommonOptions
						// throw new RuntimeException("the first arg parameter found was not Options. It was: "+argClass);
					}
					break;
				}
			}
			i++;
		}
		return i;
	}

	/**
	 * after having processed the non-CommonOptions, CollectionName, ScopeName args, starting at arg[i] 1) if the arg is
	 * an array (varargs) then use the first element of the array as the arg. This is really only necessary for
	 * CommonOptions as ScopeNames is always a vararg, and we end up making it a vararg again. 2) if this arg is the
	 * CommonOptinos, CollectionName, ScopeName arg according to clazz, then use it. If it is not, then insert whatever
	 * 'value' was supplied for that arg.
	 */
	int doNextArg(List<Object> baseArgs, Object[] args, int i, Class<?> clazz, Object value) {
		Class<?> argClass = null;
		if (args != null && i < args.length) {
			if (args[i] != null) {
				argClass = args[i].getClass();
			}
		}

		// if this is the CommonOptions or ScopeName vararg, convert it to a non-varargs arg
		// (ScopeName gets converted back to varargs)
		if (argClass != null && argClass.isArray() && (CommonOptions.class.isAssignableFrom(argClass.getComponentType()) ||
				ScopeName.class.isAssignableFrom(argClass.getComponentType()))) {
			argClass = argClass.getComponentType();
			Object[] arr = ((Object[]) args[i]);
			args[i] = arr.length > 0 ? arr[0] : null;
		}

		// if this is the arg we were looking for, past it through
		if (argClass != null && clazz.isAssignableFrom(argClass)) {
			baseArgs.add(args[i++]);
		} else { // if we didn't find it, insert the one provided fom the proxy
			baseArgs.add(value);
		}
		return i;
	}

	String debugMethod(String methodName, Object[] paramTypes) {
		return methodName + "( " + Arrays.stream(paramTypes).map(a -> String.valueOf(a)).collect(Collectors.joining(", "))
				+ " )";
	}

	String debugMethod(Method method) {
		return debugMethod(method.getName(), method.getParameterTypes());
	}
}
